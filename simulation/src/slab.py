import numpy as np
from src.schemas import Features


class SlabState:
    """
    Maintains the physical kinematic, thermodynamic, and metallurgical state of a steel slab
    as it progresses through multiple passes in a reversing hot rolling mill.

    This class enforces physical realism across the 13 raw features identified in
    Thakur et al. (2023). [https://www.sciencedirect.com/science/article/pii/S2949917823000445]

    By maintaining state across passes, it guarantees that:
    1. Volume is conserved (as thickness decreases, length must strictly increase).
    2. Thermodynamics are respected (temperature drops based on inter-pass wait times and roll contact).
    3. Kinematics scale correctly (strain rate increases dynamically based on roll speed and contact arc).
    4. Flow Stress reacts dynamically to temperature drops and strain hardening, mimicking the Zener-Hollomon parameter.
    """

    def __init__(self, rng: np.random.Generator, total_passes: int):
        """
        Initializes the fundamental geometric and thermal state of the slab as it exits the reheating furnace.

        Constants Fixed for the Slab Lifecycle:
        - reheating_time_min: Time spent in the furnace (determines core heat saturation).
        - roll_diameter_mm / roll_crown_mm: Physical dimensions of the mill stand processing this slab.
        - width_mm: Plate width (assumed constant as reversing mills primarily elongate the steel).

        Evolving State Variables (Starting Conditions):
        - thickness_mm: Initial slab thickness exiting the furnace (e.g., 200-250mm).
        - length_mm: Initial slab length (e.g., 2000-3000mm).
        - temperature_c: Initial surface temperature, highly plastic (e.g., ~1200°C).
        - speed_m_s: Initial slow entry speed.

        Args:
            rng (np.random.Generator): A seeded NumPy random generator for reproducible variance.
            total_passes (int): The predefined number of passes this slab will undergo.
        """
        self.rng = rng
        self.total_passes = total_passes
        self.current_pass = 0

        # 1. Slab/Mill Constants (Fixed for the entire lifecycle)
        self.reheating_time_min = round(self.rng.uniform(150.0, 300.0), 3)
        self.roll_diameter_mm = round(self.rng.uniform(1050.0, 1100.0), 3)
        self.roll_crown_mm = round(self.rng.uniform(30.0, 150.0), 3)
        self.width_mm = round(self.rng.uniform(1800.0, 3000.0), 3)

        # 2. Initial State Variables (Before Pass 1)
        self.thickness_mm = round(self.rng.uniform(200.0, 250.0), 3)
        self.length_mm = round(self.rng.uniform(2000.0, 3000.0), 3)
        self.temperature_c = round(self.rng.uniform(1180.0, 1220.0), 3)
        self.speed_m_s = round(self.rng.uniform(1.5, 2.0), 3)

    def next_pass(self) -> Features:
        """
        Advances the physical state of the slab by simulating a single rolling pass and
        calculates the interconnected metallurgical features.

        Physical Relationships Modeled:
        1. Drafting Schedule: Reductions are heavy in early passes (roughing) and light
        in final passes (finishing) to hit target geometries.
        2. Thermodynamics: Calculates temperature drop as a function of radiation
        (inter-pass wait time) and conduction (contact with cold rolls).
        3. Conservation of Volume: Updates the current length proportionally to the
        reduction in thickness (L_new = L_old * (H_old / H_new)).
        4. Kinematics:
        - True Strain: ln(entry_thickness / exit_thickness).
        - Contact Arc Length: sqrt(Roll_Radius * Delta_Thickness).
        - Strain Rate: (Speed / Contact_Arc) * True_Strain.
        5. Metallurgy (Flow Stress): Approximates internal resistance, which skyrockets
        as temperature drops and the metal strain-hardens.

        Args:
            wear_level (float): The current physical wear state of the rollers (0.0 to 1.0),
                                passed down from the global Digital Twin control plane.

        Returns:
            Features: A Pydantic model containing the exactly calculated 13 raw features
                    required by the Flink Online Machine Learning pipeline, strictly bounded
                    to the empirical limits observed in the reference literature.
        """
        self.current_pass += 1

        # Determine drafting schedule (reductions are heavy early, light later)
        progress_ratio = self.current_pass / self.total_passes
        if progress_ratio < 0.4:
            reduction_pct = self.rng.uniform(0.20, 0.35)  # Heavy roughing
        elif progress_ratio < 0.8:
            reduction_pct = self.rng.uniform(0.10, 0.20)  # Intermediate
        else:
            reduction_pct = self.rng.uniform(0.02, 0.10)  # Finishing

        # 1. Wait Time & Thermodynamics
        wait_time_sec = self.rng.uniform(10.0, 40.0)

        # Temp drops due to ambient air (wait time) and contact with cold rolls
        temp_drop = (
            (wait_time_sec * 0.5) + (reduction_pct * 30.0) + self.rng.normal(0, 2)
        )
        entry_temp_c = max(889.0, self.temperature_c - temp_drop)
        self.temperature_c = entry_temp_c  # Update internal state

        # 2. Geometry & Volume Conservation
        entry_thickness = self.thickness_mm
        exit_thickness = entry_thickness * (1.0 - reduction_pct)

        # Length increases proportionally as thickness decreases
        entry_length = self.length_mm
        exit_length = entry_length * (entry_thickness / exit_thickness)

        self.thickness_mm = exit_thickness
        self.length_mm = exit_length

        # 3. Kinematics (Strain & Strain Rate)
        # True strain formula: ln(h_in / h_out)
        strain = float(np.log(entry_thickness / exit_thickness))

        # Speed ramps up as the slab gets longer to clear the mill
        self.speed_m_s = min(5.6, self.speed_m_s + (reduction_pct * 2.0))

        # Contact Arc Length (L_d = sqrt(R * delta_h))
        delta_h = entry_thickness - exit_thickness
        roll_radius = self.roll_diameter_mm / 2.0
        contact_length = np.sqrt(roll_radius * delta_h)

        # Strain Rate formula: (Velocity / Contact_Length) * Strain
        # Note: speed is in m/s, length is mm, so * 1000 converts m to mm
        strain_rate = ((self.speed_m_s * 1000.0) / contact_length) * strain
        strain_rate = np.clip(strain_rate, 0.5, 16.2)  # Bound to paper limits

        # 4. Metallurgy (Flow Stress approximation)
        # Flow stress increases as temperature drops and strain/strain_rate increase
        base_stress = 20.0
        temp_factor = (
            1200.0 / entry_temp_c
        ) ** 4.0  # Cold steel is exponentially harder
        strain_hardening = (strain**0.2) * (strain_rate**0.1)

        flow_stress = base_stress * temp_factor * strain_hardening
        flow_stress = np.clip(flow_stress, 5.0, 178.0)  # Bound to paper limits

        return Features(
            reheating_time_min=self.reheating_time_min,
            roll_diameter_mm=self.roll_diameter_mm,
            roll_crown_mm=self.roll_crown_mm,
            entry_thickness_mm=round(entry_thickness, 3),
            width_mm=self.width_mm,
            length_mm=round(entry_length, 3),
            temperature_c=round(entry_temp_c, 3),
            speed_m_s=round(self.speed_m_s, 3),
            wait_time_sec=round(wait_time_sec, 3),
            reduction_pct=round(reduction_pct, 3),
            strain=round(strain, 3),
            strain_rate=round(strain_rate, 3),
            flow_stress_mpa=round(flow_stress, 3),
        )
