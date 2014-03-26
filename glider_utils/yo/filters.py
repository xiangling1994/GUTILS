from glider_utils.yo import create_profile_entry


def filter_profiles(profiles, dataset, conditional):
    """Filters out profiles that do not meet some criteria

    Returns the filtered set of profiles
    """

    filtered_profiles = []
    start_index = 0
    for profile in profiles:
        end_index = profile['index_bounds'][1]
        if conditional(profile):
            filtered_profiles.append(
                create_profile_entry(
                    dataset,
                    start_index,
                    end_index
                )
            )
            start_index = end_index
        elif len(dataset) == end_index:
            # Merge the last profile that does not meet the
            # conditional with the latest good profile.  If
            # no other profiles are qualified, make the entire
            # dataset a profile.
            if len(filtered_profiles) > 0:
                last_bounds = filtered_profiles[-1]['index_bounds']
                filtered_profiles[-1] = (
                    create_profile_entry(
                        dataset,
                        last_bounds[0],
                        end_index
                    )
                )
            else:
                filtered_profiles.append(
                    create_profile_entry(
                        dataset,
                        start_index,
                        end_index
                    )
                )

    return filtered_profiles

# Convenience methods follow


def filter_profile_depth(profiles, dataset, below=1):
    """Filters out profiles that are not below a certain depth (Default: 1m)

    Returns the filtered set of profiles
    """

    def conditional(profile):
        depth_max = max(profile['depth_bounds'])
        return depth_max >= below

    return filter_profiles(profiles, dataset, conditional)


def filter_profile_time(profiles, dataset, timespan_condition=10):
    """Filters out profiles that do not span a specified number of seconds
    (Default: 10 seconds)

    Returns the filtered set of profiles
    """

    def conditional(profile):
        timespan = profile['time_bounds'][1] - profile['time_bounds'][0]
        return timespan >= timespan_condition

    return filter_profiles(profiles, dataset, conditional)


def filter_profile_distance(profiles, dataset, distance_condition=1):
    """Filters out profiles that do not span a specified vertical distance
    (Default: 1m)

    Returns the filtered set of profiles
    """

    def conditional(profile):
        distance = abs(profile['depth_bounds'][1] - profile['depth_bounds'][0])
        return distance >= distance_condition

    return filter_profiles(profiles, dataset, conditional)


def filter_profile_number_of_points(profiles, dataset, points_condition=3):
    """Filters out profiles that do not have a specified number of points
    (Default: 3 points)

    Returns the filtered set of profiles
    """

    def conditional(profile):
        num_points = profile['index_bounds'][1] - profile['index_bounds'][0]
        return num_points >= points_condition

    return filter_profiles(profiles, dataset, conditional)
