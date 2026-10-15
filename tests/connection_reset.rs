//! Connection reset tests for a disposable SQL Server instance.

use futures_util::io::{AsyncRead, AsyncWrite};
use once_cell::sync::Lazy;
use runtimes_macro::test_on_runtimes;
use std::{env, sync::Once};
use tiberius::Result;

static LOGGER_SETUP: Once = Once::new();

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or_else(|_| {
        "server=tcp:localhost,1433;user=SA;password=<YourStrong@Passw0rd>;IntegratedSecurity=true;TrustServerCertificate=true".to_owned()
    })
});

#[derive(Debug, PartialEq)]
struct SessionState {
    database: String,
    options: i32,
    lock_timeout: i32,
    date_first: i32,
    context_info: Option<Vec<u8>>,
    session_context: Option<i32>,
}

async fn session_state<S>(conn: &mut tiberius::Client<S>) -> Result<SessionState>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let row = conn
        .query(
            "SELECT DB_NAME(), @@OPTIONS, @@LOCK_TIMEOUT, CONVERT(int, @@DATEFIRST), CONTEXT_INFO(), \
             CAST(SESSION_CONTEXT(N'tiberius_reset_test') AS int)",
            &[],
        )
        .await?
        .into_row()
        .await?
        .expect("session state returns a row");

    Ok(SessionState {
        database: row.get::<&str, _>(0).unwrap().to_owned(),
        options: row.get(1).unwrap(),
        lock_timeout: row.get(2).unwrap(),
        date_first: row.get(3).unwrap(),
        context_info: row.get::<&[u8], _>(4).map(<[u8]>::to_vec),
        session_context: row.get(5),
    })
}

#[test_on_runtimes]
async fn reset_restores_session_state<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let initial = session_state(&mut conn).await?;
    let other_database = if initial.database.eq_ignore_ascii_case("master") {
        "tempdb"
    } else {
        "master"
    };
    let date_first = initial.date_first % 7 + 1;
    let lock_timeout = if initial.lock_timeout == 1234 {
        1235
    } else {
        1234
    };

    for _ in 0..2 {
        conn.simple_query(format!(
            "USE [{other_database}]; \
             IF (@@OPTIONS & 512) = 0 SET NOCOUNT ON ELSE SET NOCOUNT OFF; \
             SET LOCK_TIMEOUT {lock_timeout}; SET DATEFIRST {date_first}; \
             SET CONTEXT_INFO 0x123456; \
             EXEC sys.sp_set_session_context @key=N'tiberius_reset_test', @value=123, @read_only=1; \
             CREATE TABLE #reset_session (id int);"
        ))
        .await?
        .into_results()
        .await?;

        let changed = session_state(&mut conn).await?;
        assert_eq!(changed.database, other_database);
        assert_ne!(changed.options & 512, initial.options & 512);
        assert_ne!(changed.lock_timeout, initial.lock_timeout);
        assert_ne!(changed.date_first, initial.date_first);
        assert_ne!(changed.context_info, initial.context_info);
        assert_eq!(changed.session_context, Some(123));
        assert_ne!(changed.session_context, initial.session_context);

        conn.reset_connection().await?;
        assert_eq!(session_state(&mut conn).await?, initial);
        let row = conn
            .query("SELECT OBJECT_ID('tempdb..#reset_session')", &[])
            .await?
            .into_row()
            .await?
            .unwrap();
        assert_eq!(row.get::<i32, _>(0), None);
    }

    Ok(())
}

#[test_on_runtimes]
async fn reset_aborts_local_transaction<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    conn.begin_transaction().await?;
    conn.simple_query("CREATE TABLE #reset_transaction (id int)")
        .await?
        .into_results()
        .await?;
    conn.execute("INSERT INTO #reset_transaction VALUES (1)", &[])
        .await?;

    let row = conn
        .query("SELECT @@TRANCOUNT", &[])
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(row.get::<i32, _>(0), Some(1));

    conn.reset_connection().await?;
    let row = conn
        .query("SELECT @@TRANCOUNT, XACT_STATE()", &[])
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(row.get::<i32, _>(0), Some(0));
    assert_eq!(row.get::<i32, _>(1), Some(0));
    let row = conn
        .query("SELECT OBJECT_ID('tempdb..#reset_transaction')", &[])
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(row.get::<i32, _>(0), None);

    conn.begin_transaction().await?;
    conn.simple_query("CREATE TABLE #after_reset (id int)")
        .await?
        .into_results()
        .await?;
    conn.execute("INSERT INTO #after_reset VALUES (42)", &[])
        .await?;
    conn.commit_transaction().await?;

    conn.begin_transaction().await?;
    conn.execute("INSERT INTO #after_reset VALUES (99)", &[])
        .await?;
    conn.rollback_transaction().await?;

    let row = conn
        .query("SELECT SUM(id), @@TRANCOUNT FROM #after_reset", &[])
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(row.get::<i32, _>(0), Some(42));
    assert_eq!(row.get::<i32, _>(1), Some(0));

    Ok(())
}

#[test_on_runtimes]
async fn reset_applies_only_to_its_request<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    conn.reset_connection().await?;
    conn.simple_query("CREATE TABLE #reset_once (id int)")
        .await?
        .into_results()
        .await?;
    conn.execute("INSERT INTO #reset_once VALUES (7)", &[])
        .await?;

    for _ in 0..2 {
        let row = conn
            .query("SELECT id FROM #reset_once", &[])
            .await?
            .into_row()
            .await?
            .unwrap();
        assert_eq!(row.get::<i32, _>(0), Some(7));
    }

    Ok(())
}
