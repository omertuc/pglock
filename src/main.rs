use std::{
    thread::{self, sleep},
    time::Duration,
};

use postgres::{Client, Error, NoTls};

const CONN: &str = "postgresql://admin:admin@localhost:5432/library";
const SAFE_SELECT: &str = "SELECT id, counter FROM counters FOR UPDATE";
const UNSAFE_SELECT: &str = "SELECT id, counter FROM counters";

struct Counter {
    _id: i32,
    counter: String,
}

fn pg_client() -> Result<Client, Error> {
    Client::connect(CONN, NoTls)
}

fn bump(client: &mut Client, safe: bool) -> Result<(), Error> {
    let mut transaction = client.transaction()?;

    let select_statement = if safe { SAFE_SELECT } else { UNSAFE_SELECT };

    for row in transaction.query(select_statement, &[])? {
        let mut counter = Counter {
            _id: row.get(0),
            counter: row.get(1),
        };
        counter.counter = (counter.counter.parse::<i64>().unwrap() + 1).to_string();
        transaction.query(
            "UPDATE counters SET counter = $2 WHERE id = $1",
            &[&counter._id, &counter.counter],
        )?;
    }

    transaction.commit()?;

    Ok(())
}

fn create_and_reset_table(client: &mut Client) -> Result<(), Error> {
    client.batch_execute("DROP TABLE IF EXISTS counters")?;

    client.batch_execute(
        "
        CREATE TABLE IF NOT EXISTS counters (
            id              SERIAL PRIMARY KEY,
            counter         VARCHAR NOT NULL
            )
    ",
    )?;

    Ok(())
}

fn initialize_counters(client: &mut Client, num_counters: i64) -> Result<(), Error> {
    for value in 0..num_counters {
        let counter = Counter {
            _id: 0,
            counter: value.to_string(),
        };

        client.execute(
            "INSERT INTO counters (counter) VALUES ($1)",
            &[&counter.counter],
        )?;
    }

    Ok(())
}

fn spawn_worker(safe: bool) {
    thread::spawn(move || {
        let mut client = pg_client().unwrap();
        for _i in 0..100 {
            bump(&mut client, safe).unwrap();
        }
    });
}

fn display_result(client: &mut Client) -> Result<(), Error> {
    for row in client.query("SELECT id, counter FROM counters", &[])? {
        let counter = Counter {
            _id: row.get(0),
            counter: row.get(1),
        };
        println!("Counter {} is {}", counter._id, counter.counter);
    }

    Ok(())
}

fn run(client: &mut Client, should_sleep: bool, safe: bool) -> Result<(), Error> {
    create_and_reset_table(client)?;
    initialize_counters(client, 1)?;

    spawn_worker(safe);
    if should_sleep {
        sleep(Duration::from_millis(500));
    }
    spawn_worker(safe);
    sleep(Duration::from_millis(500));

    println!("Running safe={safe} sleep={should_sleep}");
    display_result(client)?;

    Ok(())
}

fn main() -> Result<(), Error> {
    // Assuming that the database is running 
    // e.g. docker run --env=POSTGRES_USER=admin --env=POSTGRES_PASSWORD=admin --env=POSTGRES_DB=library -p 5432:5432 -it postgres 
    let mut client = pg_client()?;

    run(&mut client, false, false)?;
    run(&mut client, true, false)?;
    run(&mut client, false, true)?;

    Ok(())
}
