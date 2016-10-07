Release spark resource allocated by Zeppelin, achieved by restart spark interpreter through Zeppelin rest API.

Step 1 : Modify release_resource.py
-> root_url_list : zeppelin websites you want to monitor.
-> check_interval : Check zeppelin status every {check_interval} seconds.
-> release_overtime : Restart spark interpreter if Zeppelin finished after {release_overtine} seconds.

Step 2 :
Run python release_resource.py

