import pytest

from dolphie.Modules.PerformanceSchemaMetrics import PerformanceSchemaMetrics


@pytest.mark.parametrize(
    "initial_data, new_data_1, new_data_2, expected_internal_data, expected_filtered_data",
    [
        (
                [  # Initial data
                    {
                        "digest": "digest1",
                        "digest_text": "SELECT `a` FROM `mytable`",
                        "schema_name": "mydb",
                        "query_sample_text": "select /* increases in all*/ a from mytable",
                        "sum_no_good_index_used": 10,
                        "sum_no_index_used": 5,
                        "count_star": 1000,
                        "sum_errors": 1,
                        "sum_warnings": 1,
                        "sum_timer_wait": 1000000000,
                        "sum_lock_time": 1,
                        "sum_rows_sent": 10,
                        "sum_rows_examined": 10000,
                        "sum_rows_affected": 100,
                        "quantile_95": 100000,
                        "quantile_99": 1000000
                    },
                    {
                        "digest": "digest2",
                        "digest_text": "SELECT `b` FROM `mytable`",
                        "schema_name": "mydb",
                        "query_sample_text": "select /* increases on new 1 */ b from mytable",
                        "sum_no_good_index_used": 20,
                        "sum_no_index_used": 10,
                        "count_star": 2000,
                        "sum_errors": 2,
                        "sum_warnings": 2,
                        "sum_timer_wait": 2000000000,
                        "sum_lock_time": 2,
                        "sum_rows_sent": 20,
                        "sum_rows_examined": 20000,
                        "sum_rows_affected": 200,
                        "quantile_95": 200000,
                        "quantile_99": 2000000
                    },
                    {
                        "digest": "digest3",
                        "digest_text": "SELECT `c` FROM `mytable`",
                        "schema_name": "mydb",
                        "query_sample_text": "select /* increases on new 2 */ c from mytable",
                        "sum_no_good_index_used": 30,
                        "sum_no_index_used": 15,
                        "count_star": 3000,
                        "sum_errors": 3,
                        "sum_warnings": 3,
                        "sum_timer_wait": 3000000000,
                        "sum_lock_time": 3,
                        "sum_rows_sent": 30,
                        "sum_rows_examined": 30000,
                        "sum_rows_affected": 300,
                        "quantile_95": 300000,
                        "quantile_99": 3000000
                    },
                    {
                        "digest": "digest4",
                        "digest_text": "SELECT `d` FROM `mytable`",
                        "schema_name": "mydb",
                        "query_sample_text": "select /* never increases */ c from mytable",
                        "sum_no_good_index_used": 40,
                        "sum_no_index_used": 20,
                        "count_star": 4000,
                        "sum_errors": 4,
                        "sum_warnings": 4,
                        "sum_timer_wait": 4000000000,
                        "sum_lock_time": 4,
                        "sum_rows_sent": 40,
                        "sum_rows_examined": 40000,
                        "sum_rows_affected": 400,
                        "quantile_95": 400000,
                        "quantile_99": 4000000
                    },
                ],
                [  # New data 1
                    {
                        "digest": "digest1",
                        "digest_text": "SELECT `a` FROM `mytable`",
                        "schema_name": "mydb",
                        "query_sample_text": "select /* increases in all*/ a from mytable",
                        "sum_no_good_index_used": 11,
                        "sum_no_index_used": 6,
                        "count_star": 1100,
                        "sum_errors": 2,
                        "sum_warnings": 2,
                        "sum_timer_wait": 1100000000,
                        "sum_lock_time": 2,
                        "sum_rows_sent": 11,
                        "sum_rows_examined": 11000,
                        "sum_rows_affected": 110,
                        "quantile_95": 110000,
                        "quantile_99": 1100000
                    },
                    {
                        "digest": "digest2",
                        "digest_text": "SELECT `b` FROM `mytable`",
                        "schema_name": "mydb",
                        "query_sample_text": "select /* increases on new 1 */ b from mytable",
                        "sum_no_good_index_used": 22,
                        "sum_no_index_used": 11,
                        "count_star": 2200,
                        "sum_errors": 3,
                        "sum_warnings": 3,
                        "sum_timer_wait": 2200000000,
                        "sum_lock_time": 3,
                        "sum_rows_sent": 22,
                        "sum_rows_examined": 22000,
                        "sum_rows_affected": 220,
                        "quantile_95": 220000,
                        "quantile_99": 2200000
                    },
                    {
                        "digest": "digest3",
                        "digest_text": "SELECT `c` FROM `mytable`",
                        "schema_name": "mydb",
                        "query_sample_text": "select /* increases on new 2 */ c from mytable",
                        "sum_no_good_index_used": 30,
                        "sum_no_index_used": 15,
                        "count_star": 3000,
                        "sum_errors": 3,
                        "sum_warnings": 3,
                        "sum_timer_wait": 3000000000,
                        "sum_lock_time": 3,
                        "sum_rows_sent": 30,
                        "sum_rows_examined": 30000,
                        "sum_rows_affected": 300,
                        "quantile_95": 300000,
                        "quantile_99": 3000000
                    },
                    {
                        "digest": "digest4",
                        "digest_text": "SELECT `d` FROM `mytable`",
                        "schema_name": "mydb",
                        "query_sample_text": "select /* never increases */ c from mytable",
                        "sum_no_good_index_used": 40,
                        "sum_no_index_used": 20,
                        "count_star": 4000,
                        "sum_errors": 4,
                        "sum_warnings": 4,
                        "sum_timer_wait": 4000000000,
                        "sum_lock_time": 4,
                        "sum_rows_sent": 40,
                        "sum_rows_examined": 40000,
                        "sum_rows_affected": 400,
                        "quantile_95": 400000,
                        "quantile_99": 4000000
                    },
                ],
                [  # New data 2
                    {
                        "digest": "digest1",
                        "digest_text": "SELECT `a` FROM `mytable`",
                        "schema_name": "mydb",
                        "query_sample_text": "select /* increases in all*/ a from mytable",
                        "sum_no_good_index_used": 12,
                        "sum_no_index_used": 7,
                        "count_star": 1200,
                        "sum_errors": 3,
                        "sum_warnings": 3,
                        "sum_timer_wait": 1200000000,
                        "sum_lock_time": 3,
                        "sum_rows_sent": 12,
                        "sum_rows_examined": 12000,
                        "sum_rows_affected": 120,
                        "quantile_95": 120000,
                        "quantile_99": 1200000
                    },
                    {
                        "digest": "digest2",
                        "digest_text": "SELECT `b` FROM `mytable`",
                        "schema_name": "mydb",
                        "query_sample_text": "select /* increases on new 1 */ b from mytable",
                        "sum_no_good_index_used": 22,
                        "sum_no_index_used": 11,
                        "count_star": 2200,
                        "sum_errors": 3,
                        "sum_warnings": 3,
                        "sum_timer_wait": 2200000000,
                        "sum_lock_time": 3,
                        "sum_rows_sent": 22,
                        "sum_rows_examined": 22000,
                        "sum_rows_affected": 220,
                        "quantile_95": 220000,
                        "quantile_99": 2200000
                    },
                    {
                        "digest": "digest3",
                        "digest_text": "SELECT `c` FROM `mytable`",
                        "schema_name": "mydb",
                        "query_sample_text": "select /* increases on new 2 */ c from mytable",
                        "sum_no_good_index_used": 33,
                        "sum_no_index_used": 18,
                        "count_star": 3300,
                        "sum_errors": 4,
                        "sum_warnings": 4,
                        "sum_timer_wait": 3300000000,
                        "sum_lock_time": 4,
                        "sum_rows_sent": 33,
                        "sum_rows_examined": 33000,
                        "sum_rows_affected": 330,
                        "quantile_95": 330000,
                        "quantile_99": 3300000
                    },
                    {
                        "digest": "digest4",
                        "digest_text": "SELECT `d` FROM `mytable`",
                        "schema_name": "mydb",
                        "query_sample_text": "select /* never increases */ c from mytable",
                        "sum_no_good_index_used": 40,
                        "sum_no_index_used": 20,
                        "count_star": 4000,
                        "sum_errors": 4,
                        "sum_warnings": 4,
                        "sum_timer_wait": 4000000000,
                        "sum_lock_time": 4,
                        "sum_rows_sent": 40,
                        "sum_rows_examined": 40000,
                        "sum_rows_affected": 400,
                        "quantile_95": 400000,
                        "quantile_99": 4000000
                    },
                ],
                {  # Expected internal data
                    "digest1": {
                        "event_name": None,
                        "metrics": {
                            "sum_no_good_index_used": {"delta": 2, "delta_last_sample": 1, "total": 12},
                            "sum_no_index_used": {"delta": 2, "delta_last_sample": 1, "total": 7},
                            "count_star": {"delta": 200, "delta_last_sample": 100, "total": 1200},
                            "sum_errors": {"delta": 2, "delta_last_sample": 1, "total": 3},
                            "sum_warnings": {"delta": 2, "delta_last_sample": 1, "total": 3},
                            "sum_timer_wait": {"delta": 200000000, "delta_last_sample": 100000000, "total": 1200000000},
                            "sum_lock_time": {"delta": 2, "delta_last_sample": 1, "total": 3},
                            "sum_rows_sent": {"delta": 2, "delta_last_sample": 1, "total": 12},
                            "sum_rows_examined": {"delta": 2000, "delta_last_sample": 1000, "total": 12000},
                            "sum_rows_affected": {"delta": 20, "delta_last_sample": 10, "total": 120},
                        }
                    },
                    "digest2": {  # Increase on new 1
                        "event_name": None,
                        "metrics": {
                            "sum_no_good_index_used": {"delta": 2, "delta_last_sample": 0, "total": 22},
                            "sum_no_index_used": {"delta": 1, "delta_last_sample": 0, "total": 11},
                            "count_star": {"delta": 200, "delta_last_sample": 0, "total": 2200},
                            "sum_errors": {"delta": 1, "delta_last_sample": 0, "total": 3},
                            "sum_warnings": {"delta": 1, "delta_last_sample": 0, "total": 3},
                            "sum_timer_wait": {"delta": 200000000, "delta_last_sample": 0, "total": 2200000000},
                            "sum_lock_time": {"delta": 1, "delta_last_sample": 0, "total": 3},
                            "sum_rows_sent": {"delta": 2, "delta_last_sample": 0, "total": 22},
                            "sum_rows_examined": {"delta": 2000, "delta_last_sample": 0, "total": 22000},
                            "sum_rows_affected": {"delta": 20, "delta_last_sample": 0, "total": 220},
                        }
                    },
                    "digest3": {  # Increase on new 2
                        "event_name": None,
                        "metrics": {
                            "sum_no_good_index_used": {"delta": 3, "delta_last_sample": 3, "total": 33},
                            "sum_no_index_used": {"delta": 3, "delta_last_sample": 3, "total": 18},
                            "count_star": {"delta": 300, "delta_last_sample": 300, "total": 3300},
                            "sum_errors": {"delta": 1, "delta_last_sample": 1, "total": 4},
                            "sum_warnings": {"delta": 1, "delta_last_sample": 1, "total": 4},
                            "sum_timer_wait": {"delta": 300000000, "delta_last_sample": 300000000, "total": 3300000000},
                            "sum_lock_time": {"delta": 1, "delta_last_sample": 1, "total": 4},
                            "sum_rows_sent": {"delta": 3, "delta_last_sample": 3, "total": 33},
                            "sum_rows_examined": {"delta": 3000, "delta_last_sample": 3000, "total": 33000},
                            "sum_rows_affected": {"delta": 30, "delta_last_sample": 30, "total": 330},
                        }
                    },
                    "digest4": {  # Never increases
                        "event_name": None,
                        "metrics": {
                            "sum_no_good_index_used": {"delta": 0, "delta_last_sample": 0, "total": 40},
                            "sum_no_index_used": {"delta": 0, "delta_last_sample": 0, "total": 20},
                            "count_star": {"delta": 0, "delta_last_sample": 0, "total": 4000},
                            "sum_errors": {"delta": 0, "delta_last_sample": 0, "total": 4},
                            "sum_warnings": {"delta": 0, "delta_last_sample": 0, "total": 4},
                            "sum_timer_wait": {"delta": 0, "delta_last_sample": 0, "total": 4000000000},
                            "sum_lock_time": {"delta": 0, "delta_last_sample": 0, "total": 4},
                            "sum_rows_sent": {"delta": 0, "delta_last_sample": 0, "total": 40},
                            "sum_rows_examined": {"delta": 0, "delta_last_sample": 0, "total": 40000},
                            "sum_rows_affected": {"delta": 0, "delta_last_sample": 0, "total": 400},
                        }
                    }
                },
                {  # Expected filtered data
                    "digest1": {
                        "digest_text": "SELECT `a` FROM `mytable`",
                        "schema_name": "mydb",
                        "query_sample_text": "select /* increases in all*/ a from mytable",
                        "sum_no_good_index_used": {"d": 2, "d_last_sample": 1, "t": 12},
                        "sum_no_index_used": {"d": 2, "d_last_sample": 1, "t": 7},
                        "count_star": {"d": 200, "d_last_sample": 100, "t": 1200},
                        "sum_errors": {"d": 2, "d_last_sample": 1, "t": 3},
                        "sum_warnings": {"d": 2, "d_last_sample": 1, "t": 3},
                        "sum_timer_wait": {"d": 200000000, "d_last_sample": 100000000, "t": 1200000000},
                        "sum_lock_time": {"d": 2, "d_last_sample": 1, "t": 3},
                        "sum_rows_sent": {"d": 2, "d_last_sample": 1, "t": 12},
                        "sum_rows_examined": {"d": 2000, "d_last_sample": 1000, "t": 12000},
                        "sum_rows_affected": {"d": 20, "d_last_sample": 10, "t": 120},
                        "quantile_95": 120000,
                        "quantile_99": 1200000
                    },
                    "digest2": {  # Increase on new 1
                        "digest_text": "SELECT `b` FROM `mytable`",
                        "schema_name": "mydb",
                        "query_sample_text": "select /* increases on new 1 */ b from mytable",
                        "sum_no_good_index_used": {"d": 2, "d_last_sample": 0, "t": 22},
                        "sum_no_index_used": {"d": 1, "d_last_sample": 0, "t": 11},
                        "count_star": {"d": 200, "d_last_sample": 0, "t": 2200},
                        "sum_errors": {"d": 1, "d_last_sample": 0, "t": 3},
                        "sum_warnings": {"d": 1, "d_last_sample": 0, "t": 3},
                        "sum_timer_wait": {"d": 200000000, "d_last_sample": 0, "t": 2200000000},
                        "sum_lock_time": {"d": 1, "d_last_sample": 0, "t": 3},
                        "sum_rows_sent": {"d": 2, "d_last_sample": 0, "t": 22},
                        "sum_rows_examined": {"d": 2000, "d_last_sample": 0, "t": 22000},
                        "sum_rows_affected": {"d": 20, "d_last_sample": 0, "t": 220},
                        "quantile_95": 220000,
                        "quantile_99": 2200000
                    },
                    "digest3": {  # Increase on new 2
                        "digest_text": "SELECT `c` FROM `mytable`",
                        "schema_name": "mydb",
                        "query_sample_text": "select /* increases on new 2 */ c from mytable",
                        "sum_no_good_index_used": {"d": 3, "d_last_sample": 3, "t": 33},
                        "sum_no_index_used": {"d": 3, "d_last_sample": 3, "t": 18},
                        "count_star": {"d": 300, "d_last_sample": 300, "t": 3300},
                        "sum_errors": {"d": 1, "d_last_sample": 1, "t": 4},
                        "sum_warnings": {"d": 1, "d_last_sample": 1, "t": 4},
                        "sum_timer_wait": {"d": 300000000, "d_last_sample": 300000000, "t": 3300000000},
                        "sum_lock_time": {"d": 1, "d_last_sample": 1, "t": 4},
                        "sum_rows_sent": {"d": 3, "d_last_sample": 3, "t": 33},
                        "sum_rows_examined": {"d": 3000, "d_last_sample": 3000, "t": 33000},
                        "sum_rows_affected": {"d": 30, "d_last_sample": 30, "t": 330},
                        "quantile_95": 330000,
                        "quantile_99": 3300000
                    },
                }
        )
    ]
)
def test_update_internal_data_statements_summary(initial_data, new_data_1, new_data_2, expected_internal_data,
                                                 expected_filtered_data):
    p_s = PerformanceSchemaMetrics(initial_data, "statements_summary", "digest")

    p_s.update_internal_data(new_data_1)
    p_s.update_internal_data(new_data_2)

    assert p_s.internal_data == expected_internal_data
    assert p_s.filtered_data == expected_filtered_data
