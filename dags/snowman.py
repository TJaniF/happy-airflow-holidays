from airflow.decorators import dag
from airflow.models.baseoperator import chain_linear, chain
from airflow.operators.empty import EmptyOperator
from pendulum import datetime


class SnowOperator(EmptyOperator):
    ui_color = "#FFFFFF"


class BlackOperator(EmptyOperator):
    ui_color = "#000000"


class OrangeOperator(EmptyOperator):
    ui_color = "#FFA500"


class WoodOperator(EmptyOperator):
    ui_color = "#8B4513"


@dag(
    dag_display_name="☃️",
    start_date=datetime(2024, 11, 6),
    schedule="5 4 * * *",
    catchup=False,
    #orientation="TB",
)
def snowman():

    t0 = BlackOperator(task_id="t0", task_display_name="🎩")
    t1 = SnowOperator(task_id="t1", task_display_name="❄️")
    t2 = SnowOperator(task_id="t2", task_display_name="❄️")
    t3 = SnowOperator(task_id="t3", task_display_name="❄️")
    t4 = SnowOperator(task_id="t4", task_display_name="❄️")
    t5 = SnowOperator(task_id="t5", task_display_name="⚫️")
    t6 = SnowOperator(task_id="t6", task_display_name="⚫️")
    t7 = SnowOperator(task_id="t7", task_display_name="❄️")
    t8 = SnowOperator(task_id="t8", task_display_name="❄️")
    t9 = SnowOperator(task_id="t9", task_display_name="❄️")
    t10 = SnowOperator(task_id="t10", task_display_name="❄️")
    q0 = OrangeOperator(task_id="q0", task_display_name="🥕")
    q1 = SnowOperator(task_id="q1", task_display_name="❄️")
    q2 = SnowOperator(task_id="q2", task_display_name="❄️")
    q3 = SnowOperator(task_id="q3", task_display_name="❄️")
    q4 = SnowOperator(task_id="q4", task_display_name="⚫️")
    q5 = SnowOperator(task_id="q5", task_display_name="❄️")
    q6 = SnowOperator(task_id="q6", task_display_name="❄️")
    q7 = SnowOperator(task_id="q7", task_display_name="❄️")
    q8 = SnowOperator(task_id="q8", task_display_name="v")
    q9 = SnowOperator(task_id="q9", task_display_name="⚫️")
    q10 = SnowOperator(task_id="q10", task_display_name="❄️")
    l0 = WoodOperator(task_id="l0", task_display_name="🪵")
    r0 = SnowOperator(task_id="r0", task_display_name="❄️")
    r1 = SnowOperator(task_id="r1", task_display_name="❄️")
    r2 = SnowOperator(task_id="r2", task_display_name="❄️")
    r3 = SnowOperator(task_id="r3", task_display_name="❄️")
    r4 = SnowOperator(task_id="r4", task_display_name="⚫️")
    r5 = SnowOperator(task_id="r5", task_display_name="❄️")
    r6 = SnowOperator(task_id="r6", task_display_name="❄️")
    r7 = SnowOperator(task_id="r7", task_display_name="❄️")
    r8 = SnowOperator(task_id="r8", task_display_name="❄️")
    r9 = SnowOperator(task_id="r9", task_display_name="❄️")
    r10 = SnowOperator(task_id="r10", task_display_name="❄️")
    a0 = WoodOperator(task_id="a0", task_display_name="🪵")
    a1 = SnowOperator(task_id="a1", task_display_name="❄️")
    a2 = SnowOperator(task_id="a2", task_display_name="❄️")

    chain_linear(t0, [t1, t2, t3], [t4, t5, t6], [t7, t8, t9])
    chain_linear(t0, [t4, t5, t6])
    chain_linear(q0, [q1, q2, q3], [q4, q5, q6], [q7, q8, q9])
    chain_linear(q0, [q4, q5, q6])
    chain_linear(r0, [r1, r2, r3], [r4, r5, r6], [r7, r8, r9])
    chain_linear(r0, [r4, r5, r6])

    chain(t10, t7)
    chain(t9, q2)
    chain(q0, q10, q7)
    chain(q1, q8)
    chain(t0, t10)
    chain(r0, r10, r7)
    chain(r1, r8)
    chain(q10, l0, r0)
    chain(q1, a0, a1, a2)


snowman()
