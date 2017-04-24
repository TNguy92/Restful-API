


from request import request_or_fail


# #### get all

def get_all_areas():
    """
    Returns a list of dictionaries representing all the rows in the
    area table.
    """
    return request_or_fail("/area")


# #####  get by foreign key

def get_locations_for_area(area_id):
    """
    Return a list of dictionaries giving the locations for the given area.
    """
    return request_or_fail("/area/" + str(area_id) + "/location")

def get_measurements_for_location(location_id):
    """
    Return a list of dictionaries giving the measurement rows for the given location.
    """
    return request_or_fail("/location/" + str(location_id) + "/measurement")


def get_categories_for_area(area_id):
    """
    Return a list of rows from the category table that all contain the given area.
    """
    return request_or_fail("/area/" + str(area_id) + "/category")

