

from request import request_or_fail


# Path 	Function
# .../area 	Get a list of all areas
# .../area/(\d+)/location 	Get all locations for the given area id
# .../location/(\d+)/measurement 	Get all the measurements for the given location id
# .../area/(\d+)/category 	Get all the categories to which the given area belongs
# .../area/(\d+)/average_measurement 	Get the average measurement for the given area
# .../area/(\d+)/number_locations 	Get the number of locations in the given area



def get_average_measurements_for_area(area_id):
    """
    Returns the average value of all measurements for all locations in the given area.
    Returns None if there are no measurements.
    """
    return request_or_fail("/area/" + str(area_id) + "/average_measurement")

def number_of_locations_by_area(area_id):
    """
    Returns the number of locations for the given area.
    """
    return request_or_fail("/area/" + str(area_id) + "/number_locations")
