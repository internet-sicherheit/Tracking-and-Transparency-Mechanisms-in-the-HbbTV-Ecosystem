from process_har import write_har_files
from process_channels import write_channel_details
from process_meta_data import write_meta_data
from process_local_cookie_store import write_local_cookie_store

if __name__ == '__main__':
    # 81 - Profil 1 - alles
    # 82 - Profil 5 - blau
    # 83 - Profil  3 - rot
    # 84 - Profil 4 - grün
    # tbd - Profil 6 - gelb

    measurement_id = 3
    request_id = 149889 #UPDATE REQUEST_ID IF ADD DATA TO A GIVEN PROFILE
    # e.g.: SELECT MAX(request_id) FROM `hbbtv-research.hbbtv.requests` WHERE scan_profile=

    # Import the HTTP traffic from the analysis run
    #write_har_files(measurement_id, request_id)

    # Import data from the TV's cookie and local storage
    #write_local_cookie_store(measurement_id)

    # Import the log data of teh measurement run.
    write_meta_data(measurement_id)

    # Import the channel list
    #write_channel_details(measurement_id)
