# Web app to interact with Redshift, MySQL and MongoDB 
The project is an implementation of a web app which accepts an **SQL query** as input, and display the result as output. It was done as part of the project for the Rutgers course, CS 527 - Database System For Data Science.

You can find more details in this [presentation](https://github.com/jacobceles/CS527/blob/1201654c9156dbf74443db11b17939d66b0f6c69/Documents/Group%202%20-%20DataStars.pptx).

You can also view the following videos to get a better idea:
- [Explaining the dataset](https://youtu.be/9_1YUu9Q_wM)
- [Demo of the app with Redshift and MySQL](https://youtu.be/fI6mdZM8QjE)

## Features
Here are some features of this implementation:
- Retrieves data from MySQL, Redshift and MongoDB using **SQL queries**.
- Handles error cases gracefully by displaying the error message and code.
- Able to execute DDL statements and Stored procedures as well.
- Auto complete the query beign typed using suggestions.
- Download result as HTML, CSV or JSON.

### Steps:
<ol>
<li>Create an AWS account</li>
<li>Launch an EC2 micro instance</li>
<li>Create a S3 Bucket</li>
<li>Upload Instacart data to S3</li>
<li>Start a RDS (MySQL/ORACLE/SQL Server) Instance</li>
<li>Export data from S3 to MySQL</li>
<li>Start a Redshift Instance</li>
<li>Export data from S3 to Redshift</li>
<li>Start a MongoDB Instance</li>
<li>Import data into MongoDB</li>
</ol>

## Contributors
- [Jacob Celestine](https://jacobcelestine.com/)
- [Anirudh Negi](https://github.com/negiadventures)
- [Rahul Dev Ellezhuthil](https://github.com/rahuldeve)
- Rasika Hasamnis

## Screenshots
![Home Page](/Documents/screenshot1.png?raw=true "Home Page")
![Form Input](/Documents/screenshot2.png?raw=true "Form Input")
![Results](/Documents/screenshot3.png?raw=true "Results")
