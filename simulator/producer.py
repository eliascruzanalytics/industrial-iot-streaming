import os
import sys

# Ensure project root directory is in sys.path when running script directly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from simulator.mqtt_producer import run_mqtt_simulation

if __name__ == "__main__":
    run_mqtt_simulation()

