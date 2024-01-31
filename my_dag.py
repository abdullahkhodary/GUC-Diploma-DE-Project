from http.client import NO_CONTENT
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
import logging


# Check for the presence of the 'airflow' module
try:
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
except ModuleNotFoundError as e:
    print(f"Module not found: {e}")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG object
dag = DAG(
    'my_data_processing_dag',
    default_args=default_args,
    description='A simple data processing DAG',
    schedule_interval=timedelta(days=1),  # Adjust as needed
)


def check_and_install_libraries(**kwargs):
    try:
        import seaborn
        import matplotlib
    except ImportError:
        # If seaborn or matplotlib is not installed, install them
        import subprocess
        subprocess.run(["pip", "install", "seaborn", "matplotlib"])
        logging.info("seaborn and matplotlib installed successfully.")
    else:
        logging.info("seaborn and matplotlib are already installed.")







# Define a function to load data
def load_data(**kwargs):
    df = pd.read_csv('/opt/airflow/dags/Uncleaned_DS_jobs.csv')
    kwargs['ti'].xcom_push(key='my_data', value=df)
    print(df)

    return df






# Define a function for data cleaning
def data_cleaning(df_task_instance, **kwargs):

    # Inside data_cleaning_task
    df = kwargs['ti'].xcom_pull(task_ids='load_data_task', key='my_data')
    
    df.drop(columns=['index', 'Competitors'], inplace=True)

    # Handling missing values in 'Rating' column
    df['Rating'].replace(-1, 0, inplace=True)

    # Handling missing values in various columns
    columns_with_missing_values = ['Headquarter', 'Size', 'Founded', 'Type of Ownership', 'Industry', 'Sector', 'Revenue']
    df.replace([-1, '-1', 'Unknown'], np.nan, inplace=True)

    # Tidy up the column names
    df.rename(columns={'Job Title': 'Job Title', 'Salary Estimate': 'Salary Estimate',
                       'Job Description': 'Job Description', 'Company Name': 'Company Name',
                       'Type of ownership': 'Ownership'}, inplace=True)

    # Drop duplicates
    df.drop_duplicates(inplace=True, keep='first')

    # Handling outliers in 'Rating' column using IQR method
    Q1 = df['Rating'].quantile(0.25)
    Q3 = df['Rating'].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR

    df['Rating'] = np.where((df['Rating'] < lower) | (df['Rating'] > upper), np.nan, df['Rating'])

    # Handling outliers in 'Founded' column using IQR method
    Q1 = df['Founded'].quantile(0.25)
    Q3 = df['Founded'].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR

    df['Founded'] = np.where((df['Founded'] < lower) | (df['Founded'] > upper), np.nan, df['Founded'])

    # Convert 'Founded' column to integer type
    df['Founded'] = df['Founded'].astype('Int64')
    kwargs['ti'].xcom_push(key='my_data', value=df)
    print(df)

    return df







# Define a function for feature engineering
def feature_engineering(df_task_instance, **kwargs):
    import pandas as pd
    import numpy as np
    import re

    # Inside feature_engineering_task
    df = kwargs['ti'].xcom_pull(task_ids='data_cleaning_task', key='my_data')
    
    
    # Cleaning for Salary Estimate Column
    
    if df is not None:
        df['Salary Estimate'] = df['Salary Estimate'].str.replace('K', '')
        df['Salary Estimate'] = df['Salary Estimate'].str.replace('$', '')
        df['Salary Estimate'] = df['Salary Estimate'].str.replace('-', '-')
        df['Salary Estimate'] = df['Salary Estimate'].apply(lambda x: x.split('(')[0])
        df['Minimum Salary'] = df['Salary Estimate'].apply(lambda x: int(x.split('-')[0]))
        df['Maximum Salary'] = df['Salary Estimate'].apply(lambda x: int(x.split('-')[1]))
        df['Average Salary'] = (df['Maximum Salary'] + df['Minimum Salary']) // 2


    # Categorizing salary into bins
    if df is not None:
        bins = [0, 100, 125, 175, float('inf')]
        labels = ['Very Low', 'Low', 'Medium', 'High']
        df['salary_category'] = pd.cut(df['Average Salary'], bins=bins, labels=labels, right=False)

    # Creating bins for Rating
    if df is not None:
        bins = [0, 1, 2, 3, 4, 5, np.inf]
        labels = ['0-1', '1-2', '2-3', '3-4', '4-5', 'NaN']
        df['Rating Interval'] = pd.cut(df['Rating'], bins=bins, labels=labels, include_lowest=True)

    # Size column cleaning and creating Min and Max Employees columns
    if df is not None:
        df['Size'] = df['Size'].str.replace('to', '-')
        df['Size'] = df['Size'].str.replace('employees', '')
        df['Size'] = df['Size'].str.replace('employees', '')
        df['Size'] = df['Size'].replace(np.nan, 'Unknown')
        df['Min Employees'] = df['Size'].apply(lambda x: x.split('-')[0])
        df['Max Employees'] = df['Size'].apply(lambda x: x.split('-')[-1])
        df['Min Employees'] = df['Min Employees'].str.replace('10000+', '10001')

    # Ownership categorization

    if df is not None:
        public_ownership = ['Government', 'Company - Public']
        private_ownership = ['Subsidiary or Business Segment', 'Private Practice / Firm', 'Company - Private']
        non_profit = ['College / University', 'Hospital', 'Nonprofit Organization']
        other_ownership = ['Self-employed', 'Contract', 'Other Organization']

        df['Ownership'] = df['Ownership'].replace('-1', 'Unknown')

        df['Ownership Category'] = 'Other'
        df.loc[df['Ownership'].isin(public_ownership), 'Ownership Category'] = 'Public'
        df.loc[df['Ownership'].isin(private_ownership), 'Ownership Category'] = 'Private'
        df.loc[df['Ownership'].isin(non_profit), 'Ownership Category'] = 'Nonprofit'

    # One-Hot Encoding Ownership
    if df is not None:
        encoded_df = pd.get_dummies(df['Ownership'], prefix='Ownership')
        df = pd.concat([df, encoded_df], axis=1)

    # Data Extraction from Job Description
        
    # Educational Level categorization    
    if df is not None:
        education_pattern = r'(bachelor|master|phd|doctorate|msc|bcs|Bachelor\\s|masters|Advanced degree|ph.d|master\\s|mba)'
        df['Education Level'] = df['Job Description'].str.extract(education_pattern, flags=re.IGNORECASE)
        df['Education Level'] = df['Education Level'].str.replace('ph d', 'Phd', case=False)
        df['Education Level'] = df['Education Level'].str.replace('ph.d', 'Phd', case=False)
        df['Education Level'] = df['Education Level'].str.replace('msc', 'Master', case=False)
        df['Education Level'] = df['Education Level'].str.replace('mba', 'Master', case=False)
        df['Education Level'] = df['Education Level'].str.replace('Doctorate', 'Phd', case=False)
        df['Education Level'] = df['Education Level'].fillna("Not Mentioned")

    # Experience Level categorization
    if df is not None:
        conditions = [
            (df['Job Description'].str.contains('1+', case=False)) | (df['Job Description'].str.contains("at least 1", case=False)),
            (df['Job Description'].str.contains('3+', case=False)),
            (df['Job Description'].str.contains('lead', case=False)) | (df['Job Description'].str.contains('senior', case=False))
    ]
        choices = ['Entry', 'Mid-Senior', 'Senior']
        df['Experience Level'] = np.select(conditions, choices, default='Not Mentioned')

    # Boolean indicators for specific skills in Job Description
    if df is not None:
        df['python'] = df['Job Description'].str.contains('python', case=False).astype(int)
        df['excel'] = df['Job Description'].str.contains('excel', case=False).astype(int)
        df['hadoop'] = df['Job Description'].str.contains('hadoop', case=False).astype(int)
        df['spark'] = df['Job Description'].str.contains('spark', case=False).astype(int)
        df['aws'] = df['Job Description'].str.contains('aws', case=False).astype(int)
        df['tableau'] = df['Job Description'].str.contains('tableau', case=False).astype(int)
    
    if df is not None:
        df.drop(columns=['Job Description'], inplace=True)

    if df is not None:
       df['State'] = df['Location'].apply(lambda x: x.split(',')[-1])
       df['State'] = df['State'].str.replace('United States', 'US')
       df = df[~((df['State'] == 'Remote') | (df['State'] == 'Utah') |
                  (df['State'] == 'New Jersey') | (df['State'] == 'Texas') |
                  (df['State'] == 'California'))]
       

    if df is not None:
        df['Company Name'] = df['Company Name'].str.split('\n').str[0] 


    kwargs['ti'].xcom_push(key='my_data', value=df)
    
    return df










# function for analyzing salary correlation
def analyze_salary_correlation(**kwargs):
    import pandas as pd
    import seaborn as sns
    import matplotlib.pyplot as plt
    df = kwargs['ti'].xcom_pull(task_ids='feature_engineering_task', key='my_data')

    # Correlation analysis
    correlation_data = df[['Rating', 'Average Salary']]
    correlation_coefficient = correlation_data.corr().iloc[0, 1]

    # Display the correlation coefficient
    print(f"Pearson Correlation Coefficient between Rating and Average Salary: {correlation_coefficient}")

    # Scatter plot
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x='Rating', y='Average Salary', data=correlation_data, alpha=0.7)
    plt.title('Scatter Plot of Rating vs. Average Salary')
    plt.xlabel('Rating')
    plt.ylabel('Average Salary')
    plt.grid(True)
    plt.savefig('/opt/airflow/dags/scatter_plot.png')












    # Define the function for visualizations
def create_visualizations(**kwargs):
    import pandas as pd  # Import necessary libraries
    import matplotlib.pyplot as plt
    import seaborn as sns
    # Assuming 'filtered_df' is available in the global scope
    df = kwargs['ti'].xcom_pull(task_ids='feature_engineering_task', key='my_data')

    plt.figure(figsize=(12, 12))
    sns.barplot(x="State", y="Average Salary", data=df)
    plt.xticks(rotation=45)
    plt.title("State vs. Average Salary")
    plt.savefig('/opt/airflow/dags/state_vs_salary.png')  # Save the plot to a file
    plt.close()

    # Visualize average salary across different industries (Sectors)
    plt.figure(figsize=(20, 12))
    sns.barplot(x="Sector", y="Average Salary", data=df)
    plt.xticks(rotation=45)
    plt.title("Sector vs. Average Salary")
    plt.savefig('/opt/airflow/dags/sector_vs_salary.png')  
    plt.close()

    # Visualize skills by experience level
    skills = ['python', 'excel', 'hadoop', 'spark', 'aws', 'tableau']
    fig, axes = plt.subplots(nrows=2, ncols=4, figsize=(16, 10), sharey=True)
    fig.suptitle('Comparison of Skills by Experience Level', fontsize=16)

    axes = axes.flatten()

    for i, skill in enumerate(skills):
        sns.barplot(x='Experience Level', y=skill, data=df, ax=axes[i])
        axes[i].set_title(skill)
        axes[i].set_xlabel('Experience Level')
        axes[i].set_ylabel(skill)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig('/opt/airflow/dags/skills_vs_experience.png')  
    plt.close()











def export_to_csv_function(**kwargs):
    try:
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='feature_engineering_task', key='my_data')

        if df is not None:
            output_csv_path = '/opt/airflow/dags/final.csv'
            df.to_csv(output_csv_path, index=False)
            logging.info(f"Exported data to {output_csv_path}")
        else:
            logging.warning("No data available to export.")
    except Exception as e:
        logging.error(f"Error exporting to CSV: {e}")
        raise      










import logging
from datetime import datetime

def document_dag():
    """Documentation

    Purpose:
    This DAG processes data from Uncleaned_DS_jobs.csv, performing data cleaning and feature engineering
    to prepare the dataset for further analysis or machine learning tasks.

    Steps/Tasks:
    1. `data_cleaning_task`: Reads the raw data from 'Uncleaned_DS_jobs.csv' and performs initial cleaning.
        - Input: 'Uncleaned_DS_jobs.csv'
        - Output: Cleaned DataFrame

    2. `feature_engineering_task`: Applies feature engineering to the cleaned data, creating new features and
       categorizing existing ones. This includes handling Salary Estimates, Ownership, Job Description details,
       and additional features like educational and experience levels, skills, etc.
        - Input: Cleaned DataFrame from `data_cleaning_task`
        - Output: Feature-engineered DataFrame

    Input Data:
    - 'Uncleaned_DS_jobs.csv': The raw dataset containing job postings with data to be cleaned and processed.

    Output:
    - Modified and feature-engineered dataset saved for further analysis or downstream tasks.

    Dependencies:
    - The DAG ensures that `data_cleaning_task` is executed before `feature_engineering_task` to maintain
      the correct order of data processing.
    """

    # Get current date and time
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Define the file path
    file_path = f"/opt/airflow/dags/dag_documentation_{current_datetime}.txt"

    # Write documentation to the file
    with open(file_path, "w") as file:
        file.write(document_dag.__doc__)  # Using the docstring as content

    logging.info(f"DAG documentation saved at: {file_path}")












# Define tasks for each step in the DAG
load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    dag=dag,
)

# Task to clean data
data_cleaning_task = PythonOperator(
    task_id='data_cleaning_task',
    python_callable=data_cleaning,
    provide_context=True,
    op_args=[],
    op_kwargs={'df_task_instance': '{{ task_instance.xcom_pull(task_ids="load_data_task") }}'},
    dag=dag,
)
# Task to analyze salary correlation
analyze_salary_correlation_task = PythonOperator(
    task_id='analyze_salary_correlation_task',
    python_callable=analyze_salary_correlation,
    provide_context=True,
    dag=dag,
)

# Task to perform feature engineering
feature_engineering_task = PythonOperator(
    task_id='feature_engineering_task',
    python_callable=feature_engineering,
    provide_context=True,
    op_args=[],
    op_kwargs={'df_task_instance': '{{ task_instance.xcom_pull(task_ids="data_cleaning_task") }}'},
    dag=dag,
)

# Define the task for exporting to CSV
export_to_csv_task = PythonOperator(
    task_id='export_to_csv_task',
    python_callable=export_to_csv_function,
    provide_context=True,
    op_kwargs={'df_task_instance': '{{ task_instance.xcom_pull(task_ids="feature_engineering_task") }}'},
    dag=dag,
)



documentation_task = PythonOperator(
    task_id='documentation_task',
    python_callable=document_dag,
    dag=dag,
)

install_libraries_task = PythonOperator(
    task_id='install_libraries_task',
    python_callable=check_and_install_libraries,
    dag=dag,
)

# Define the task for visualizations
create_visualizations_task = PythonOperator(
    task_id='create_visualizations_task',
    python_callable=create_visualizations,
    dag=dag,
)




[documentation_task, install_libraries_task] >> load_data_task >> data_cleaning_task >> feature_engineering_task >> export_to_csv_task
feature_engineering_task >> [analyze_salary_correlation_task, create_visualizations_task]






