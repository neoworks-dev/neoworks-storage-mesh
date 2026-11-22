//! Warning accounts by email before their data expires.
//!
//! Data in the mesh is deleted as soon as nobody pays for it, and nodes cannot
//! warn anyone, since they see only anonymous tabs. The coordinator can: it
//! knows each account's email and whether the account took its tokens this
//! month. Tokens are signed blind, so it cannot tell which data is whose,
//! only that an account stopped paying, which is enough to warn.
//!
//! An account that took tokens last month and none this month gets a first
//! warning once the month is [`CoordinatorSettings::first_warning_after`]
//! old, and a final one [`CoordinatorSettings::final_warning_before`] the end
//! of the month, when tabs paid with last month's tokens run out at the
//! latest. Warnings sent are recorded in `<data>/warnings.sqlite3`, so a
//! restart never sends one twice.
//!
//! The coordinator does not speak SMTP. It writes each email as a file in
//! `<data>/outbox/`, for the system's mail tool to send, which keeps mail
//! credentials and delivery problems out of the coordinator.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use mesh_core::tokens::{MONTH_SECONDS, month_of};
use rusqlite::{Connection, params};
use tokio::sync::Mutex;

use crate::clock::unix_now;
use crate::settings::CoordinatorSettings;
use crate::token_issuer::TokenIssuer;

/// Which of the two warnings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WarningKind {
    First,
    Final,
}

impl WarningKind {
    fn name(self) -> &'static str {
        match self {
            WarningKind::First => "first",
            WarningKind::Final => "final",
        }
    }
}

/// The warnings sent so far, and where new ones go.
pub struct ExpiryWarnings {
    connection: Mutex<Connection>,
    outbox: PathBuf,
}

impl ExpiryWarnings {
    /// Opens the record of sent warnings and the outbox in `directory`.
    pub fn open(directory: &Path) -> std::io::Result<Self> {
        let outbox = directory.join("outbox");
        std::fs::create_dir_all(&outbox)?;
        let connection =
            Connection::open(directory.join("warnings.sqlite3")).map_err(std::io::Error::other)?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS sent (
                     email TEXT NOT NULL,
                     month INTEGER NOT NULL,
                     kind  TEXT NOT NULL,
                     PRIMARY KEY (email, month, kind)
                 );",
            )
            .map_err(std::io::Error::other)?;
        Ok(ExpiryWarnings {
            connection: Mutex::new(connection),
            outbox,
        })
    }

    /// Writes every warning due at `now` to the outbox, and returns how many
    /// there were.
    pub async fn send_due(
        &self,
        tokens: &TokenIssuer,
        settings: &CoordinatorSettings,
        now: u64,
    ) -> std::io::Result<usize> {
        let month = month_of(now);
        let Some(kind) = due_kind(settings, now) else {
            return Ok(0);
        };
        let lapsed = tokens
            .lapsed_accounts(month)
            .await
            .map_err(std::io::Error::other)?;
        let expires_at = (month + 1) * MONTH_SECONDS;
        let mut sent = 0;
        for email in lapsed {
            if !self.mark_sent(&email, month, kind).await? {
                continue;
            }
            self.write_email(&email, kind, expires_at, now)?;
            sent += 1;
        }
        Ok(sent)
    }

    /// Records that `kind` went to `email` this `month`; `false` when it had
    /// already.
    async fn mark_sent(&self, email: &str, month: u64, kind: WarningKind) -> std::io::Result<bool> {
        let connection = self.connection.lock().await;
        let inserted = connection
            .execute(
                "INSERT OR IGNORE INTO sent (email, month, kind) VALUES (?1, ?2, ?3)",
                params![email, month, kind.name()],
            )
            .map_err(std::io::Error::other)?;
        Ok(inserted == 1)
    }

    /// Writes one email to the outbox, through a temporary name, so a mail
    /// tool never picks up half of one.
    fn write_email(
        &self,
        email: &str,
        kind: WarningKind,
        expires_at: u64,
        now: u64,
    ) -> std::io::Result<()> {
        let date = utc_date(expires_at);
        let subject = match kind {
            WarningKind::First => format!("Your NeoWorks storage expires on {date}"),
            WarningKind::Final => format!("Last reminder: your NeoWorks storage expires on {date}"),
        };
        let body = format!(
            "Hello,\n\n\
             your account has not taken its storage tokens this month, for example\n\
             because a payment failed. Everything you keep in the mesh is deleted\n\
             on {date} unless the account pays again before then.\n\n\
             NeoWorks cannot see what you store, so this message is all we can tell\n\
             you about it.\n"
        );
        let message = format!("To: {email}\r\nSubject: {subject}\r\n\r\n{body}");
        let name = format!(
            "{now}-{}-{}.eml",
            kind.name(),
            blake3::hash(email.as_bytes()).to_hex()
        );
        let temporary = self.outbox.join(format!(".{name}.partial"));
        std::fs::write(&temporary, message)?;
        std::fs::rename(&temporary, self.outbox.join(name))
    }
}

/// The warning due at `now`, if the month is far enough along for one.
fn due_kind(settings: &CoordinatorSettings, now: u64) -> Option<WarningKind> {
    let month_start = month_of(now) * MONTH_SECONDS;
    let month_end = month_start + MONTH_SECONDS;
    if now + settings.final_warning_before.as_secs() >= month_end {
        return Some(WarningKind::Final);
    }
    if now >= month_start + settings.first_warning_after.as_secs() {
        return Some(WarningKind::First);
    }
    None
}

/// Sends due warnings every warning interval, forever.
pub async fn warn_forever(
    warnings: ExpiryWarnings,
    tokens: Arc<TokenIssuer>,
    settings: CoordinatorSettings,
) {
    let mut timer = tokio::time::interval(settings.warning_interval);
    loop {
        timer.tick().await;
        // A failed round is retried at the next tick; nothing was marked sent
        // that was not written.
        if let Err(error) = warnings.send_due(&tokens, &settings, unix_now()).await {
            eprintln!("expiry warnings: {error}");
        }
    }
}

/// `unix_seconds` as a UTC date, such as `2026-12-01`.
fn utc_date(unix_seconds: u64) -> String {
    // Howard Hinnant's days-to-civil algorithm, for days since 1970-01-01.
    let days = (unix_seconds / 86_400) as i64 + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn settings() -> CoordinatorSettings {
        CoordinatorSettings {
            first_warning_after: Duration::from_secs(7 * 86_400),
            final_warning_before: Duration::from_secs(3 * 86_400),
            ..CoordinatorSettings::default()
        }
    }

    #[test]
    fn dates_are_written_as_years_months_and_days() {
        assert_eq!(utc_date(0), "1970-01-01");
        assert_eq!(utc_date(1_795_310_400), "2026-11-22");
        assert_eq!(utc_date(951_782_400), "2000-02-29");
    }

    #[test]
    fn the_first_warning_waits_a_week_and_the_final_comes_at_the_end() {
        let month_start = 600 * MONTH_SECONDS;

        assert_eq!(due_kind(&settings(), month_start + 86_400), None);
        assert_eq!(
            due_kind(&settings(), month_start + 8 * 86_400),
            Some(WarningKind::First)
        );
        assert_eq!(
            due_kind(&settings(), month_start + 28 * 86_400),
            Some(WarningKind::Final)
        );
    }

    #[tokio::test]
    async fn a_lapsed_account_is_warned_once_of_each_kind() {
        let directory = tempfile::tempdir().unwrap();
        let tokens = TokenIssuer::open(directory.path()).unwrap();
        let warnings = ExpiryWarnings::open(directory.path()).unwrap();
        tokens
            .record_issued("ana@example.org", 599, 10)
            .await
            .unwrap();
        tokens
            .record_issued("bob@example.org", 599, 10)
            .await
            .unwrap();
        tokens
            .record_issued("bob@example.org", 600, 10)
            .await
            .unwrap();
        let month_start = 600 * MONTH_SECONDS;

        let first = warnings
            .send_due(&tokens, &settings(), month_start + 8 * 86_400)
            .await
            .unwrap();
        let again = warnings
            .send_due(&tokens, &settings(), month_start + 9 * 86_400)
            .await
            .unwrap();
        let last = warnings
            .send_due(&tokens, &settings(), month_start + 28 * 86_400)
            .await
            .unwrap();

        assert_eq!((first, again, last), (1, 0, 1));
        let mails: Vec<String> = std::fs::read_dir(directory.path().join("outbox"))
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect();
        assert_eq!(mails.len(), 2);
        assert!(
            mails
                .iter()
                .all(|mail| mail.starts_with("To: ana@example.org\r\n"))
        );
    }
}
