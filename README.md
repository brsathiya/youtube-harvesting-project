# YouTube Data Harvesting and Warehousing using SQL, MongoDB, and Streamlit

## Project Overview

This project involves the development of a Streamlit application that empowers users to access and analyze data from multiple YouTube channels. The application integrates various technologies and skills including Python scripting, data collection, MongoDB, Streamlit, API integration, and data management using MongoDB (Atlas) and SQL. It operates within the domain of Social Media.

## Problem Statement

The problem statement for this project can be summarized as follows:

**Objective:** Create a Streamlit application that enables users to access and analyze data from multiple YouTube channels.

**Key Features:**
- Ability to input a YouTube channel ID and retrieve all relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using the Google API.
- Option to store the collected data in a MongoDB database as a data lake.
- Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
- Option to select a channel name and migrate its data from the data lake to a SQL database as tables.
- Ability to search and retrieve data from the SQL database using various search options, including joining tables to obtain channel details.

## Approach

The project can be divided into the following phases:

1. **Set up a Streamlit App:**
   - Streamlit is chosen as the platform to build a user-friendly interface for the project. Users can input a YouTube channel ID, view channel details, and select channels for data migration to the data warehouse.

2. **Connect to the YouTube API:**
   - Utilize the YouTube API to retrieve channel and video data. The Google API client library for Python is used to make API requests.

3. **Store Data in a MongoDB Data Lake:**
   - Once data is retrieved from the YouTube API, it is stored in a MongoDB data lake. MongoDB is an ideal choice for this purpose due to its capacity to handle unstructured and semi-structured data.

4. **Migrate Data to a SQL Data Warehouse:**
   - After collecting data for multiple channels, migrate it to a SQL data warehouse. SQL databases like MySQL or PostgreSQL are suitable for this purpose.

5. **Query the SQL Data Warehouse:**
   - SQL queries are employed to join tables in the SQL data warehouse and retrieve data for specific channels based on user input. Libraries like SQLAlchemy can be used for interaction with the SQL database.

6. **Display Data in the Streamlit App:**
   - Visualize the retrieved data within the Streamlit app. Utilize Streamlit's data visualization features to create charts and graphs for in-depth data analysis.

## Getting Started

Follow these instructions to run the project locally:

1. Install the necessary Python libraries and packages using `pip`. You can create a virtual environment for this project to manage dependencies.

   ```bash
   pip install -r requirements.txt

   Skills Acquired
By working on this project, you will gain proficiency in the following skills:

Python scripting
Data collection and integration with APIs
MongoDB for data storage
Streamlit for creating interactive data visualization applications
Data management using MongoDB (Atlas)
Working with SQL databases for data warehousing
SQL for querying and analyzing data
Effective data presentation and visualization
This project serves as a valuable opportunity to integrate various technologies, develop a practical application, and enhance skills that are directly applicable in the domain of social media data analysis. It enables users to harness the power of data and derive meaningful insights from YouTube channels.

Feel free to contribute, modify, or enhance this project as needed. Happy coding!
