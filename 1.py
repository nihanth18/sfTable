import streamlit as st
import snowflake.connector
import pandas as pd
from faker import Faker
import toml  # Import the toml library to parse the TOML file

fake = Faker()

def generate_data():
    data = {}
    column_names = []

    for _ in range(10):
        column_name = fake.word().replace(' ', '_')[:20]
        column_names.append(column_name)

    for column_name in column_names:
        column_values = []
        for _ in range(10):
            column_value = fake.word()
            column_values.append(column_value)
        data[column_name] = column_values

    return data

data = [generate_data() for _ in range(10)]

# Read Snowflake configuration from config.toml
config_data = toml.load("config.toml")
snowflake_config = config_data["SNOWFLAKE"]

def get_snowflake_connection():
    return snowflake.connector.connect(**snowflake_config)

st.title("Generate and Save Data to Snowflake")

st.write("Generated Data:")
generated_data_df = pd.DataFrame(data[0])  # Assuming all generated data has the same structure

st.write(generated_data_df)

if st.button("Save Data to Snowflake"):
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # Drop the existing Snowflake table if it exists
        drop_table_sql = "DROP TABLE IF EXISTS generated_data"
        cursor.execute(drop_table_sql)
        
        # Create Snowflake table dynamically based on the column names
        create_table_sql = "CREATE TABLE generated_data ("
        for column_name in generated_data_df.columns:
            create_table_sql += f'{column_name} STRING, '
        create_table_sql = create_table_sql.rstrip(', ') + ")"
        cursor.execute(create_table_sql)

        # Insert data into the Snowflake table
        for _, row in generated_data_df.iterrows():
            insert_sql = "INSERT INTO generated_data ("
            values_sql = "VALUES ("
            for column_name in generated_data_df.columns:
                insert_sql += f'{column_name}, '
                values_sql += f'\'{row[column_name]}\', '
            insert_sql = insert_sql.rstrip(', ') + ") "
            values_sql = values_sql.rstrip(', ') + ")"
            cursor.execute(insert_sql + values_sql)

        conn.commit()
        st.success("Data saved to Snowflake successfully!")
    except Exception as e:
        st.error(f"Error saving data to Snowflake: {str(e)}")
    finally:
        conn.close()
