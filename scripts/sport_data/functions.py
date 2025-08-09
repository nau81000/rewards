"""
Helper functions to manage activities 
"""
from airflow.decorators import task

@task(task_id="get_activities_from_strava")
def get_activities_from_strava ():
    """ Get activities from Strava
    """

@task(task_id="generate_activities")
def generate_activities_from_scratch ():
    """ Generate activities from scratch into a SQlite file
    """
    print('generate_activities_from_scratch')

@task(task_id="check_activities")
def check_activities ():
    """ Check activities from scratch into a SQlite file
    """
    print('check_activities')
