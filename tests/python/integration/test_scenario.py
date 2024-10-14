from dku_plugin_test_utils import dss_scenario


TEST_PROJECT_KEY = "PLUGINTESTSAPODATA"


def test_run_sap_odata_read(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="SAP")


def test_run_sap_odata_v4(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="SAP_V4")
