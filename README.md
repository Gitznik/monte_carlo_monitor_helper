# Monte Carlo Monitor Helper

## Summary
This project aims to make setting [Monte Carlo](https://www.montecarlodata.com) field tracking monitors for entire Snowflake schemas easier and automatable.

For using this project you need:

* A Snowflake database you want to enable field health tracking for
    * A JDBC/ODBC user with read access to the information schema of the snowflake database
    * One or more specific schemas you want to enable field health tracking for
* [Monte Carlo](https://www.montecarlodata.com)

## Getting Started
To install the dependencies, run `pip3 install -r requirements.txt` from the project directory.

You need to set up a file called `snowflake_secrets.yaml` in the `./utility/config` directory. You can find a blueprint for this file in the same directory. 

In the `./utility/config` directory you can also find a `mc_monitor_config` file, where you can specify the time fields you want to monitor on (prioritized top to bottom), and the schemas to monitor on.

The script expects the passoword for the snowflake user to be saved as an environment variable called `SNOWFLAKE_PW`. If you want to provide the snowflake password in a different way, you can pass it directly into the query_snowflake function in `get_mcons_to_update`. 

## Run the project
To prevent accidental setting of monitors, the project runs in 2 stages. 

1. Get mcons to update: 
2. Set Monte Carlo monitors: