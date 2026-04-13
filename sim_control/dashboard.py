import logging
import clickhouse_connect
from nicegui import ui, run
from dynamic_des import KafkaAdminConnector

from src.config import (
    KAFKA_BROKER,
    TOPIC_CONTROL_INGRESS,
    CH_HOST,
    CH_PORT,
    CH_USER,
    CH_DB,
)

# ==========================================
# Logging Configuration
# ==========================================
logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)
logger = logging.getLogger("sim_control.dashboard")

try:
    ch_client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, database=CH_DB
    )
    logger.info("Connected to ClickHouse")
except Exception as e:
    logger.error("ClickHouse Connection Failed: %s", e)
    ch_client = None

admin = KafkaAdminConnector(bootstrap_servers=KAFKA_BROKER, max_tasks=200)

GRADE_MAPPING = {
    "structural": "STRUCTURAL",
    "microalloyed": "MICROALLOYED",
    "high_alloy": "HIGH ALLOY",
}


# ==========================================
# Control Plane Actions
# ==========================================
async def apply_drift_config(grade, drift_type, value, freq):
    try:
        if drift_type == "abrupt":
            await admin.send_config(
                TOPIC_CONTROL_INGRESS,
                f"HotRolling.variables.velocity_{grade}",
                {"type": "abrupt", "value": 0.0},
            )
            await admin.send_config(
                TOPIC_CONTROL_INGRESS,
                f"HotRolling.containers.wear_{grade}.current_cap",
                float(value),
            )
            ui.notify(
                f"Abrupt Update: {GRADE_MAPPING[grade]} wear set to {value:.2f}",
                type="positive",
            )
        elif drift_type == "gradual":
            payload = {"type": "gradual", "value": float(value), "freq": int(freq)}
            await admin.send_config(
                TOPIC_CONTROL_INGRESS, f"HotRolling.variables.velocity_{grade}", payload
            )
            ui.notify(
                f"Gradual Update: {GRADE_MAPPING[grade]} drift started", type="positive"
            )
    except Exception as e:
        logger.error(f"Failed to send Kafka config: {e}")
        ui.notify("Failed to send command to Kafka", type="negative")


async def handle_apply(g, m, v, f):
    # Send the command
    await apply_drift_config(g, m.value, v.value, f.value)

    if m.value == "abrupt":
        v.value = 0.0
    else:
        v.value = 0
        f.value = 0


# ==========================================
# Dashboard UI
# ==========================================
@ui.page("/")
def index():
    ui.query("body").style("background-color: #f8fafc")
    ui.colors(primary="#1E3A8A", secondary="#10B981", accent="#F59E0B")

    with ui.header().classes(
        "items-center justify-center bg-primary text-white p-4 shadow-md"
    ):
        with ui.row().classes("w-full max-w-6xl justify-between items-center"):
            ui.label("Hot Rolling Digital Twin - Concept Drift Monitor").classes(
                "text-2xl font-bold"
            )

    with ui.column().classes("w-full max-w-6xl mx-auto mt-4 gap-4"):
        with ui.tabs().classes(
            "w-full bg-slate-200 text-slate-700 font-bold rounded-t-lg"
        ) as tabs:
            tab_refs = {grade: ui.tab(label) for grade, label in GRADE_MAPPING.items()}

        charts = {}
        wear_labels = {}
        series_selections = {}

        # Standard definition for dynamic chart lines
        series_definitions = {
            "Baseline": {
                "name": "Baseline Error",
                "type": "line",
                "itemStyle": {"color": "#EF4444"},
                "animation": False,
            },
            "Target Mean": {
                "name": "Target Mean Error",
                "type": "line",
                "itemStyle": {"color": "#F59E0B"},
                "animation": False,
            },
            "SGD": {
                "name": "SGD Error",
                "type": "line",
                "itemStyle": {"color": "#10B981"},
                "animation": False,
            },
            "AMRules": {
                "name": "AMRules Error",
                "type": "line",
                "itemStyle": {"color": "#8B5CF6"},
                "animation": False,
            },
        }

        with ui.tab_panels(tabs, value=tab_refs["structural"]).classes(
            "w-full bg-transparent p-0"
        ):
            for grade in GRADE_MAPPING:
                with ui.tab_panel(tab_refs[grade]).classes("p-0 gap-4 flex flex-col"):
                    # --- CONTROL PANEL ---
                    with ui.row().classes(
                        "w-full items-end p-4 bg-white rounded-lg shadow-sm border border-slate-200 gap-6"
                    ):
                        with ui.column().classes("gap-1"):
                            ui.label("Drift Type:").classes(
                                "font-semibold text-slate-700 text-sm"
                            )
                            mode_select = ui.select(
                                {
                                    "abrupt": "Abrupt (Instant)",
                                    "gradual": "Gradual (Auto)",
                                },
                                value="abrupt",
                            ).classes("w-40")

                        with ui.column().classes("gap-1"):
                            ui.label().bind_text_from(
                                mode_select,
                                "value",
                                backward=lambda v: (
                                    "Wear (0-100):" if v == "abrupt" else "Step Amount:"
                                ),
                            ).classes("font-semibold text-slate-700 text-sm")
                            val_input = ui.number(
                                value=0.0, format="%.2f", step=0.5
                            ).classes("w-28")

                        with (
                            ui.column()
                            .classes("gap-1")
                            .bind_visibility_from(
                                mode_select, "value", lambda v: v == "gradual"
                            )
                        ):
                            ui.label("Freq (sec):").classes(
                                "font-semibold text-slate-700 text-sm"
                            )
                            freq_input = ui.number(
                                value=0, format="%d", step=1
                            ).classes("w-20")

                        with ui.column().classes("gap-1 pb-1"):
                            ui.button(
                                "Apply",
                                on_click=lambda g=grade, m=mode_select, v=val_input, f=freq_input: (
                                    handle_apply(g, m, v, f)
                                ),
                            ).props("color=primary outline")

                        # Multi-Select for Chart Lines
                        with ui.column().classes(
                            "gap-1 border-l-2 border-slate-200 pl-6"
                        ):
                            ui.label("Metrics to Plot:").classes(
                                "font-semibold text-slate-700 text-sm"
                            )
                            series_selections[grade] = ui.select(
                                options=["Baseline", "Target Mean", "SGD", "AMRules"],
                                value=["Baseline", "Target Mean", "SGD", "AMRules"],
                                multiple=True,
                            ).classes("w-64")

                        ui.space()

                        # Live Wear Indicator
                        with ui.column().classes(
                            "gap-1 items-end p-2 bg-slate-50 rounded border border-slate-200"
                        ):
                            ui.label("Current Wear").classes(
                                "text-xs text-slate-500 uppercase font-bold"
                            )
                            wear_labels[grade] = ui.label("0.00").classes(
                                "text-3xl font-mono font-black text-slate-500"
                            )

                    # --- CHART ---
                    charts[grade] = ui.echart(
                        {
                            "animation": False,
                            "tooltip": {"trigger": "axis"},
                            "legend": {"data": [], "bottom": 0},
                            "grid": {
                                "left": "5%",
                                "right": "5%",
                                "bottom": "15%",
                                "top": "15%",
                                "containLabel": True,
                            },
                            "xAxis": {
                                "type": "category",
                                "boundaryGap": False,
                                "data": [],
                            },
                            "yAxis": {
                                "type": "value",
                                "name": "APE (%)",
                                "scale": True,
                            },
                            "series": [],
                        }
                    ).classes(
                        "w-full h-[500px] bg-white border border-slate-200 rounded-lg shadow-sm p-4"
                    )

    # ==========================================
    # Async Update Loop
    # ==========================================
    async def fetch_and_update():
        if not ch_client:
            return

        for grade in GRADE_MAPPING:
            # Added am_rules_ape to the SQL query
            query = f"""
                SELECT formatDateTime(evaluation_timestamp, '%M:%d\n%H:%S'), 
                       baseline_ape, target_mean_ape, sgd_ape, am_rules_ape,
                       wear_level, is_am_rules_fallback
                FROM dev.oml_evaluation_metrics 
                WHERE steel_grade = '{grade}' 
                ORDER BY evaluation_timestamp DESC 
                LIMIT 60
            """
            try:
                result = await run.io_bound(ch_client.query, query)
                rows = list(reversed(result.result_rows))

                if rows:
                    active_keys = series_selections[grade].value
                    series_data = []
                    legend_data = []

                    # 2. Extract the coordinates for the stars
                    fallback_stars = []
                    for r in rows:
                        timestamp = r[0]
                        am_rules_ape_val = round(r[4], 2)
                        is_fallback = r[6]

                        # If fallback triggered, create a star markPoint
                        if is_fallback == 1:
                            fallback_stars.append(
                                {
                                    "coord": [timestamp, am_rules_ape_val],
                                    "symbol": "star",
                                    "symbolSize": 16,
                                    "itemStyle": {
                                        "color": "#EF4444"
                                    },  # Bright red star!
                                }
                            )

                    for key in active_keys:
                        series_template = series_definitions[key].copy()
                        if key == "Baseline":
                            series_template["data"] = [round(r[1], 2) for r in rows]
                        elif key == "Target Mean":
                            series_template["data"] = [round(r[2], 2) for r in rows]
                        elif key == "SGD":
                            series_template["data"] = [round(r[3], 2) for r in rows]
                        elif key == "AMRules":
                            series_template["data"] = [round(r[4], 2) for r in rows]

                            # 3. Attach the stars strictly to the AMRules line
                            if fallback_stars:
                                series_template["markPoint"] = {
                                    "data": fallback_stars,
                                    "animation": False,
                                }

                        series_data.append(series_template)
                        legend_data.append(series_template["name"])

                    charts[grade].options["xAxis"]["data"] = [r[0] for r in rows]
                    charts[grade].options["series"] = series_data
                    charts[grade].options["legend"]["data"] = legend_data

                    wear_labels[grade].set_text(f"{rows[-1][5]:.2f}")
                    charts[grade].update()
            except Exception as e:
                logger.error("ClickHouse error on %s: %s", grade, e)

    ui.timer(2.0, fetch_and_update)


if __name__ in {"__main__", "__mp_main__"}:
    ui.run(title="Digital Twin Dashboard", port=8080, show=False, reload=True)
