# kumparde

A simple ELT pipeline to replicate articles data from MySQL to Redshift.
This project uses AWS because the author chose to play safe and thorough by using cloud technology she's most familiar with.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Features](#features)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgements](#acknowledgements)
- [Contact](#contact)

## Architecture

### RDS MySQL 
- Version: 8.0.35
- Accessible via Bastion Host

### EC2 for Bastion Host
- Public IP: 43.218.240.111
- Accessible only from a whitelisted IP Address


## Data

A new database `kumparde` is created.

Dummy data is sourced from Kaggle: [Articles.csv](https://www.kaggle.com/datasets/asad1m9a9h6mood/news-articles?resource=download).
Then imported via an IDE (PyCharm) to RDS table `kumparde.articles_raw`.

Since the dummy data does not exactly match the requirement, another table `kumparde.articles` is created to include more columns like `id`, `author_id`, `created_at`, `updated_at`, and `deleted_at`. 
Here is the query to create table `kumparde.articles`.
```sql
create table kumparde.articles
(
    id           INT NOT NULL AUTO_INCREMENT,
    title        varchar(1000),
    content      text,
    published_at date,
    author_id    int,
    created_at   datetime,
    updated_at   datetime,
    deleted_at   datetime,
    PRIMARY KEY (id)
);

INSERT INTO kumparde.articles(
    title,
    content,
    published_at,
    author_id,
    created_at,
    updated_at,
    deleted_at
)
SELECT
    heading as title,
    article as content,
    STR_TO_DATE(published_date, '%m/%d/%Y') as published_at,
    FLOOR(RAND() * 500) + 1 AS author_id,
    TIMESTAMP(DATE_SUB(STR_TO_DATE(published_date, '%m/%d/%Y'), INTERVAL 1 DAY), SEC_TO_TIME(FLOOR(RAND() * 86400))) AS created_at,
    CASE
        WHEN MOD((FLOOR(RAND() * 500) + 1), 20) = 0 THEN
            TIMESTAMP(DATE_ADD(STR_TO_DATE(published_date, '%m/%d/%Y'), INTERVAL 1 DAY), SEC_TO_TIME(FLOOR(RAND() * 86400)))
        ELSE NULL
    END AS updated_at,
    CASE
        WHEN MOD((FLOOR(RAND() * 500) + 1), 83) = 0 THEN
            TIMESTAMP(DATE_ADD(STR_TO_DATE(published_date, '%m/%d/%Y'), INTERVAL 1 DAY), SEC_TO_TIME(FLOOR(RAND() * 86400)))
        ELSE NULL
    END AS deleted_at
FROM
    kumparde.articles_raw;
```
*Note that `ELSE NULL` is unnecessary. But on more than one occasion, the author forgot what the default value for `ELSE` is. So she chose to include it for good measure.

### Installation Steps

1. Clone the repository:
    ```bash
    git clone https://github.com/username/project-name.git
    cd project-name
    ```
2. Install dependencies:
    ```bash
    npm install
    ```
3. Set up the database:
    ```bash
    mysql -u root -p < database/schema.sql
    ```
4. Run the application:
    ```bash
    npm start
    ```

## Usage

Instructions and examples for using the project.

```bash
# Basic usage example
node app.js
