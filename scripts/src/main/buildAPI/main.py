
from flask import Flask
from flask import request
from config import config
import pyodbc

app = Flask(__name__)

@app.route('/app/v1/transactions/batch', methods=['POST'])

def insert_transactions():
    data = request.json

    if not isinstance(data, dict):
        return 'Invalid data format. Expected a dictionary.', 400
    
    if 'employees' not in data or 'departments' not in data or 'jobs' not in data:
        return 'Missing required data for employees or departments or jobs.', 400
    
    employees_data = data.get('employees')
    departments_data = data.get('departments')
    jobs_data = data.get('jobs')

    if not isinstance(employees_data, list) or not isinstance(departments_data, list) or not isinstance(jobs_data, list):
        return 'Invalid data format. Expected a list of rows for employees, departments and jobs.', 400
    
    if len(employees_data) < 1 or len(employees_data) > 1000:
        return 'Invalid number of rows for employees. Must be between 1 and 1000.', 400
    
    if len(departments_data) < 1 or len(departments_data) > 1000:
        return 'Invalid number of rows for employees. Must be between 1 and 1000.', 400
    
    if len(jobs_data) < 1 or len(jobs_data) > 1000:
        return 'Invalid number of rows for employees. Must be between 1 and 1000.', 400   

    inserted_employees = []
    inserted_departments = []
    inserted_jobs = []

    #Create a connection to the Azure SQL database
 
    conn = pyodbc.connect(app.config['connection_string'])
    cursor = conn.cursor()

    try:
        for employee_row in employees_data:
            if not validate_employee_row(employee_row):
                return 'Invalid data format in employees row.', 400

            inserted_employees.append(employee_row)
        
        # insert data into table
            cursor.execute("""
                INSERT INTO employees (id, name, datetime, department_id, job_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                employee_row['id'], employee_row['name'], employee_row['datetime'], employee_row['department_id'], employee_row['job_id']
            ) 

        for department_row in departments_data:
            if not validate_department_row(department_row):
                return 'Invalid data format in departments row.', 400

            inserted_departments.append(department_row)

            cursor.execute("""
                INSERT INTO departments (id, department)
                VALUES (?, ?)
                """,
                department_row['id'], department_row['department']
            )

        for job_row in jobs_data:
            if not validate_department_row(job_row):
                return 'Invalid data format in departments row.', 400

            inserted_jobs.append(job_row)

            cursor.execute("""
                INSERT INTO jobs (id, job)
                VALUES (?, ?)
                """,
                job_row['id'], job_row['job']
            )

        conn.commit()  
        return {
        'message': 'Batch transactions inserted successfully',
        'inserted_employees': inserted_employees,
        'inserted_departments': inserted_departments,
        'inserted_jobs': inserted_jobs
        }, 200   
    
    except Exception as e:
        conn.rollback()
        return f"An error occurred: {str(e)}", 500
    finally:
        conn.close()

          

def validate_employee_row(row):
    # Implement your validation logic for employees table based on the data rules
    if not isinstance(row, dict):
        return False

    required_fields = ['id', 'name', 'datetime', 'department_id', 'job_id']
    if not all(field in row for field in required_fields):
        return False

    # Additional validation rules for each field if needed

    return True

def validate_department_row(row):
    # Implement your validation logic for departments table based on the data rules
    if not isinstance(row, dict):
        return False

    required_fields = ['id', 'department']
    if not all(field in row for field in required_fields):
        return False

    # Additional validation rules for each field if needed

    return True

def validate_job_row(row):
    # Implement your validation logic for departments table based on the data rules
    if not isinstance(row, dict):
        return False

    required_fields = ['id', 'job']
    if not all(field in row for field in required_fields):
        return False

    # Additional validation rules for each field if needed

    return True


if __name__ == '__main__':
    app.config.from_object(config['development'])
    app.run()