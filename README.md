# Using SQL queries to interact with Redshift, SQL and MongoDB 
The project is an implementation of a web app which accepts an SQL query as input, and display the result as output. It was done as part of the project for the Rutgers course, CS 527 - Database System For Data Science.

## Features
Here are some features of this implementation:
- Retrieves data from MySQL, Redshift and MongoDB using _SQL queries_
- Handles error cases gracefully by displaying the error message and code
- Able to execute DDL statements and Stored procedures as well
- Autocomplete the query beign typed using suggestions
- Download result as HTML, CSV or JSON

You can find more details in this [presenation](https://github.com/jacobceles/CS527/blob/1201654c9156dbf74443db11b17939d66b0f6c69/Documents/Group%202%20-%20DataStars.pptx).

You can also view the following videos to get a better idea:
- [Explaining the dataset](https://youtu.be/9_1YUu9Q_wM)
- [Demo of the app with Redshift and MySQL](https://youtu.be/fI6mdZM8QjE)

### Steps involved:
Create an AWS account
Launch an EC2 micro instance
Create a S3 Bucket
Upload Instacart data to S3
Start a RDS (MySQL/ORACLE/SQL Server) Instance
Export data from S3 to MySQL
Start a Redshift Instance
Export data from S3 to Redshift
Start a MongoDB Instance
Import data into MongoDB

## Contributors
- [Jacob Celestine](https://jacobcelestine.com/)
- [Anirudh Negi](https://github.com/negiadventures)
- [Rahul Dev Ellezhuthil](https://github.com/rahuldeve)
- Rasika Hasamnis

## Screenshots
![Home Page](/Documents/screenshot1.png?raw=true "Home Page")
![Form Input](/Documents/screenshot2.png?raw=true "Form Input")
![Results](/Documents/screenshot3.png?raw=true "Results")