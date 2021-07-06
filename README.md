
# Monte Carlo Monitor Helper

This project aims to make setting [Monte Carlo](https://www.montecarlodata.com) field tracking monitors for entire Snowflake schemas easier and automatable.

[![GPLv3 License](https://img.shields.io/badge/License-GPL%20v3-yellow.svg)](https://opensource.org/licenses/)
## Documentation

The default behavior of this project is setup to work with a snowflake database. You can write your own version of the database_connection class to work for your database.

For using this project you need:

* A database you want to enable field health tracking for
    * A JDBC/ODBC user with read access to the information schema of the database
    * One or more specific schemas you want to enable field health tracking for
* [Monte Carlo](https://www.montecarlodata.com)
## Features

- Set field health tracking automatically for all tables in a schema, if they are not yet monitored
- Only set tracking for pre defined time fields
- Set trackig for tables without time fields

  
## Installation

Install the dependencies with `pip3 install -r requirements.txt` from the project directory.

You need to set up a file called `database_secrets.yaml` in the `./utility/config` directory. You can find a blueprint for this file based on the snowflake connection in the same directory. 

In the `./utility/config` directory you can also find a `mc_monitor_config` file, where you can specify the time fields you want to monitor on (prioritized top to bottom), and the schemas to monitor on.
## Environment Variables

To run this project, you will need to add the following environment variables to your environment

`SNOWFLAKE_PW` if you're using the provided snowflake connection

`X_MCD_ID` for the monte carlo API

`X_MCD_TOKEN` for the monte carlo API

  