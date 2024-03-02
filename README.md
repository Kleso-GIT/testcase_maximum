# Analytics Solution

This repository contains a data analysis solution implemented in Python for comparing data processed using pandas and
SQL.

## Getting Started

### Prerequisites

Make sure you have Python installed on your system. You can install the required dependencies using the following
command:

```bash
pip install -r requirements.txt
```
### Configuration
Before running the solution, configure the database connection details in the .env file. Set the USER and PASSWORD environment variables with the appropriate values.

### .env
```
USER=your_username
PASSWORD=your_password
```
### Running the Solution
Execute the main.py script to compare data processed using pandas and SQL. The script reads data from a PostgreSQL database and outputs whether the processed dataframes are equal or not.

```bash
python main.py
```
### Files
```markdown
**main.py**: The main script that orchestrates the data processing and comparison.

**pandas_solution.py**: Contains the pandas-based solution for data processing.

**sql_solution.py**: Implements the SQL-based solution for data processing.

**requirements.txt**: Specifies the Python dependencies for the solution.
```