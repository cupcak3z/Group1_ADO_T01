# Introduction
dbt Core is an open sourced project where you can develop from the command line and run your dbt project. This documentation is for the Operating System Windows.

## Set-Up
Below is the reference for where the general steps come from (dbt core)
[dbt core installation](https://docs.getdbt.com/docs/core/installation-overview)

### Prerequisites
- User has to have a working IDE (e.g. Visual Studio Code)
- A Snowflake Account
- Python
- Access to a terminal or command prompt
- Have pip installed
- Necessary permissions to create directories and install packages on your machine
- Ensure that the raw Contoso data is in a Snowflake database
Do note that you might have to adapt some of the code to meet your requirements

### Windows Command Prompt
1. Create a virual environment using the code: `py -m venv env`
Do note that if you already have a virtual environment created you can use it, then you can skip the above step
2. Activate your virtual environment using the code: `env\Scripts\activate`
3. If you want to deactivate your virtual environment, input this code: `deactivate`
4. Next, we have to install the Adapter. The code below downloads both dbt-core and the adapter we will be using, which is dbt-snowflake
Code: `python -m pip install dbt-core dbt-snowflake`
5. After which you will have to create a new project, you can follow the steps provided in the command line from this point onwards. You may refer to the profiles.yml file and the dbt_project.yml file for a brief idea on how to structure your inputs.

### IDE Terminal
1. Clone the repository in the environment that dbt-core was installed, which is the current environment, using the code: `git clone https://github.com/cupcak3z/Group1_ADO_T01.git`
2. Update the code in the profile.yml file and the dbt_project.yml file within the repository to fit your situation (e.g. snowflake account name, or snowflake database)
3. Once done, you can input the code `dbt debug` to check if your connection is successful
4. If successful, you can conduct dbt tests using the code: `dbt test`
5. To run the models, you can use the code to create the tables in your Snowflake database: `dbt run`
