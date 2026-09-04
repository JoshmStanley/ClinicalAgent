-- One database per service. Runs once when the postgres volume is first created.
CREATE DATABASE identity;
CREATE DATABASE conversations;
CREATE DATABASE financials;
CREATE DATABASE documents;
