from airflow.decorators import dag
from airflow.models.baseoperator import chain, chain_linear
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from pendulum import datetime


class StarOperator(EmptyOperator):
    ui_color = "#D4AF37"


class NeedlesOperator(EmptyOperator):
    ui_color = "#228B22"


class TrunkOperator(EmptyOperator):
    ui_color = "#8B4513"


@dag(
    dag_display_name="🎄",
    start_date=datetime(2024, 11, 6),
    schedule="5 4 * * *",
    catchup=False,
    orientation="TB",
)
def christmas_tree():

    star = StarOperator(task_id="t0", task_display_name="🌟")
    t1 = NeedlesOperator(task_id="t1", task_display_name="🔴")
    t2a = NeedlesOperator(task_id="t2a", task_display_name="Happy Holidays!")
    t2b = NeedlesOperator(task_id="t2b", task_display_name="🐿️")
    t2c = NeedlesOperator(task_id="t2c", task_display_name="💜")
    t3a = NeedlesOperator(task_id="t3a", task_display_name="🏵️")
    t3b = NeedlesOperator(task_id="t3b", task_display_name="💝")
    t3c = NeedlesOperator(task_id="t3c", task_display_name="🔴")
    t4a = NeedlesOperator(task_id="t4a", task_display_name="⭐️")
    t4b = NeedlesOperator(task_id="t4b", task_display_name="✨")
    t4c = NeedlesOperator(task_id="t4c", task_display_name="🔴")
    t4d = NeedlesOperator(task_id="t4d", task_display_name="🌕")
    t4e = NeedlesOperator(task_id="t4e", task_display_name="🧡")
    t4f = NeedlesOperator(task_id="t4f", task_display_name="🔴")
    t4g = NeedlesOperator(task_id="t4g", task_display_name="❄️")
    t5a = NeedlesOperator(task_id="t5a", task_display_name="🔴")
    t5b = NeedlesOperator(task_id="t5b", task_display_name="⭐️")
    t7a = NeedlesOperator(task_id="t7a", task_display_name="✨")
    t7b = NeedlesOperator(task_id="t7b", task_display_name="🔴")
    t7c = TrunkOperator(task_id="t7c", task_display_name="🪵")
    t6a = NeedlesOperator(task_id="t6a", task_display_name="🌼")

    chain_linear(star, t1, [t2a, t2b, t2c], [t3a, t3b, t3c])
    chain_linear(t1, t3b)
    chain(t2a, t2b, t2c)
    chain(t3a, t4a)
    chain(t3b, t4b)
    chain(t3c, t4c)
    chain(t4c, t4d)
    chain(t4c, t4e)
    chain(t4b, t4f)
    chain(t4b, t4g)
    chain(t4d, [t5a, t5b])
    chain_linear([t3c, t3a, t3b], [t4c, t4a, t4b])
    chain(t1, Label("❄️"), t3b)
    chain(t1, Label("✨"), t3c)
    chain(t4c, t6a)
    chain(t4e, t7a)
    chain(t5b, t7b)
    chain(t5a, t7c)


christmas_tree()
