from typing import Any, Dict, List


class PerformanceSchemaDeltaTracker:
    def __init__(self, query_data: List[Dict[str, Any]]):
        """
        Initializes the tracker to store initial data and cumulative sums for each file name.
        """
        self.initial_data: Dict[str, Dict[str, int]] = {}
        self.cumulative_sums: Dict[str, Dict[str, int]] = {}

        for row in query_data:
            file_name = row["FILE_NAME"]
            # Initialize each metric for this file
            self.initial_data[file_name] = {}
            self.cumulative_sums[file_name] = {}
            for metric, value in row.items():
                if metric != "FILE_NAME":  # Exclude FILE_NAME from metrics
                    self.initial_data[file_name][metric] = value
                    # Initialize cumulative sums for each metric for this file
                    self.cumulative_sums[file_name][metric] = 0

    def update_cumulative_sums(self, query_data: Dict[str, Dict[str, int]]):
        """
        Updates cumulative sums by calculating deltas from the initial snapshot for each file name.

        :param query_data: List of dictionaries containing the current snapshot of metrics for each file name.
        """
        for row in query_data:
            file_name = row["FILE_NAME"]

            # If we have initial data for this file, calculate deltas
            if file_name in self.initial_data:
                for metric, current_value in row.items():
                    if metric != "FILE_NAME" and metric in self.initial_data[file_name]:
                        initial_value = self.initial_data[file_name][metric]
                        delta = current_value - initial_value

                        # Only add positive deltas to cumulative sums for this file
                        if delta > 0:
                            # Initialize the file_name entry if it doesn't exist
                            if file_name not in self.cumulative_sums:
                                self.cumulative_sums[file_name] = {}

                            # Initialize the metric entry if it doesn't exist
                            if metric not in self.cumulative_sums[file_name]:
                                self.cumulative_sums[file_name][metric] = 0

                            # Add the delta to cumulative sums
                            self.cumulative_sums[file_name][metric] += delta

            # Update the initial data to reflect the current snapshot for next delta calculation
            self.initial_data[file_name] = {metric: value for metric, value in row.items() if metric != "FILE_NAME"}

    def get_cumulative_sums(self) -> Dict[str, Dict[str, int]]:
        """
        Returns the cumulative sums for each file name, excluding metrics with a cumulative sum of 0.

        :return: Dictionary of cumulative sums for each metric, organized by file name, with non-zero values only.
        """
        filtered_sums = {}

        for file_name, metrics in self.cumulative_sums.items():
            # Filter out metrics with a cumulative sum of 0 for each file
            non_zero_metrics = {metric: value for metric, value in metrics.items() if value != 0}

            if non_zero_metrics:  # Only include files with non-zero metrics
                filtered_sums[file_name] = non_zero_metrics

        return filtered_sums
