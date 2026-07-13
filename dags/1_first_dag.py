from airflow.sdk import dag, task 


@dag(
        # Jadikan fungsi ini sebagai satu rangkaian alur kerja utama.
        dag_id="first_dag",
)

def first_dag():

    # Jadikan fungsi ini sebagai pekerja mandiri yang mengeksekusi kode python.
    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")
    
    @task.python
    def third_task():
        print("This is the third task. DAG complete!")
    
    
    # Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    
    first >> second >> third

# Instantiating the DAG
first_dag()