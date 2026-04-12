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
    """Sends the correct configuration to the simulation based on the drift type."""
    try:
        if drift_type == "abrupt":
            # 1. Stop any gradual drift by zeroing out the velocity
            await admin.send_config(
                TOPIC_CONTROL_INGRESS,
                f"HotRolling.variables.velocity_{grade}",
                {"type": "abrupt", "value": 0.0},
            )
            # 2. Directly set the absolute wear level in the container
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
            # Set the velocity variable using your updated dictionary schema
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


# ==========================================
# Dashboard UI
# ==========================================
@ui.page("/")
def index():
    # Subtle background color for the whole page
    ui.query("body").style("background-color: #f8fafc")
    ui.colors(primary="#1E3A8A", secondary="#10B981", accent="#F59E0B")

    # Header spanning full width
    with ui.header().classes(
        "items-center justify-center bg-primary text-white p-4 shadow-md"
    ):
        with ui.row().classes("w-full max-w-5xl justify-between items-center"):
            ui.label("Hot Rolling Digital Twin - Multi-Grade Monitor").classes(
                "text-2xl font-bold"
            )

    # Main Content Wrapper - Centered and narrowed!
    with ui.column().classes("w-full max-w-5xl mx-auto mt-4 gap-4"):
        # Top-Level Tabs
        with ui.tabs().classes(
            "w-full bg-slate-200 text-slate-700 font-bold rounded-t-lg"
        ) as tabs:
            tab_refs = {grade: ui.tab(label) for grade, label in GRADE_MAPPING.items()}

        charts = {}
        wear_labels = {}

        with ui.tab_panels(tabs, value=tab_refs["structural"]).classes(
            "w-full bg-transparent p-0"
        ):
            for grade in GRADE_MAPPING:
                with ui.tab_panel(tab_refs[grade]).classes("p-0 gap-4 flex flex-col"):
                    # --- CONTROL PANEL ---
                    with ui.row().classes(
                        "w-full items-end p-4 bg-white rounded-lg shadow-sm border border-slate-200 gap-6"
                    ):
                        # Type Selector
                        with ui.column().classes("gap-1"):
                            ui.label("Drift Type:").classes(
                                "font-semibold text-slate-700 text-sm"
                            )
                            mode_select = ui.select(
                                {
                                    "abrupt": "Abrupt (Instant)",
                                    "gradual": "Gradual (Oscillating)",
                                },
                                value="abrupt",
                            ).classes("w-48")

                        # Value/Step Input (Changes context based on type)
                        with ui.column().classes("gap-1"):
                            ui.label().bind_text_from(
                                mode_select,
                                "value",
                                backward=lambda v: (
                                    "Target Wear (0-100):"
                                    if v == "abrupt"
                                    else "Step Amount:"
                                ),
                            ).classes("font-semibold text-slate-700 text-sm")
                            val_input = ui.number(
                                value=0.0, format="%.2f", step=0.5
                            ).classes("w-32")

                        # Frequency Input (Only shows if gradual)
                        with (
                            ui.column()
                            .classes("gap-1")
                            .bind_visibility_from(
                                mode_select, "value", lambda v: v == "gradual"
                            )
                        ):
                            ui.label("Frequency (sec):").classes(
                                "font-semibold text-slate-700 text-sm"
                            )
                            freq_input = ui.number(
                                value=5, format="%d", step=1
                            ).classes("w-24")

                        # Apply Button
                        with ui.column().classes("gap-1 pb-1"):
                            ui.button(
                                "Apply Config",
                                on_click=lambda g=grade, m=mode_select, v=val_input, f=freq_input: (
                                    apply_drift_config(g, m.value, v.value, f.value)
                                ),
                            ).props("color=accent")

                        ui.space()

                        # Live Wear Indicator
                        with ui.column().classes(
                            "gap-1 items-end p-2 bg-slate-50 rounded border border-slate-200"
                        ):
                            ui.label("Current Wear").classes(
                                "text-xs text-slate-500 uppercase font-bold"
                            )
                            wear_labels[grade] = ui.label("0.00").classes(
                                "text-3xl font-mono font-black text-red-500"
                            )

                    # --- CHART ---
                    charts[grade] = ui.echart(
                        {
                            "animation": False,  # <--- DISABLES THE SLIDING ANIMATION
                            "tooltip": {"trigger": "axis"},
                            "legend": {
                                "data": [
                                    "Baseline Error",
                                    "Target Mean Error",
                                    "SGD Error",
                                ],
                                "bottom": 0,
                            },
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
                                "splitNumber": 5,
                            },
                            "series": [
                                {
                                    "name": "Baseline Error",
                                    "type": "line",
                                    "itemStyle": {"color": "#EF4444"},
                                    "data": [],
                                },
                                {
                                    "name": "Target Mean Error",
                                    "type": "line",
                                    "itemStyle": {"color": "#F59E0B"},
                                    "data": [],
                                },
                                {
                                    "name": "SGD Error",
                                    "type": "line",
                                    "itemStyle": {"color": "#10B981"},
                                    "data": [],
                                },
                            ],
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

        # Query all grades so charts remain populated even when tabs are hidden
        for grade in GRADE_MAPPING:
            query = f"""
                SELECT formatDateTime(evaluation_timestamp, '%H:%M:%S'), baseline_ape, target_mean_ape, sgd_ape, wear_level
                FROM dev.oml_evaluation_metrics WHERE steel_grade = '{grade}' ORDER BY evaluation_timestamp DESC LIMIT 60
            """
            try:
                result = await run.io_bound(ch_client.query, query)
                rows = list(reversed(result.result_rows))

                if rows:
                    charts[grade].options["xAxis"]["data"] = [r[0] for r in rows]
                    charts[grade].options["series"][0]["data"] = [
                        round(r[1], 2) for r in rows
                    ]
                    charts[grade].options["series"][1]["data"] = [
                        round(r[2], 2) for r in rows
                    ]
                    charts[grade].options["series"][2]["data"] = [
                        round(r[3], 2) for r in rows
                    ]

                    wear_labels[grade].set_text(f"{rows[-1][4]:.2f}")
                    charts[grade].update()
            except Exception as e:
                logger.error("ClickHouse error on %s: %s", grade, e)

    ui.timer(2.0, fetch_and_update)


if __name__ in {"__main__", "__mp_main__"}:
    ui.run(title="Digital Twin Dashboard", port=8080, show=False, reload=False)
