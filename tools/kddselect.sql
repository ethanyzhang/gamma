select normalYN, is_host_login, is_guest_login, logged_in, land, duration, src_bytes, dst_bytes,
       wrong_fragment, urgent, hot, num_failed_logins, num_compromised, root_shell, su_attempted,
       num_root, num_file_creations, num_shells, num_access_files, count, srv_count, serror_rate,
       srv_serror_rate, rerror_rate, srv_rerror_rate, same_srv_rate, diff_srv_rate,
       srv_diff_host_rate, dst_host_count, dst_host_srv_count, dst_host_same_srv_rate,
       dst_host_diff_srv_rate, dst_host_same_src_port_rate,  dst_host_srv_diff_host_rate,
       dst_host_serror_rate, dst_host_srv_serror_rate, dst_host_rerror_rate, dst_host_srv_rerror_rate,
CASE
    WHEN attack = 'pod.' then 99
    WHEN attack = 'neptune.' then 75
    WHEN attack = 'warezclient.' then 64
    WHEN attack = 'back.' then 56
    WHEN attack = 'nmap.' then 52
    WHEN attack = 'portsweep.' OR attack='ipsweep.' OR attack ='guess_passwd.' then 41
    WHEN attack = 'satan.' then 25
    WHEN attack = 'teardrop.' then 20
    WHEN attack = 'smurf.' then 7
    WHEN attack = 'normal.' then 0
    ELSE 1 
END
FROM complex.KDDnet_n000010M_d000100_H 