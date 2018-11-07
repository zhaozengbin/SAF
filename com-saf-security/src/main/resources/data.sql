insert into sec_user(username,password) values('admin','admin');
insert into sec_user(username,password) values('test','test');
insert into sec_role(rolename,prompt) values('ROLE_USER','common user privilege');
insert into sec_role(rolename,prompt) values('ROLE_ADMIN','administrator privilege');
insert into sec_role_user(user_id,role_id) values(2,1);
insert into sec_role_user(user_id,role_id) values(1,1);
insert into sec_role_user(user_id,role_id) values(1,2);