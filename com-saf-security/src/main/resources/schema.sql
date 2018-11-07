create table sec_user(
 id int(10) primary key auto_increment,
 username varchar(100),
 password varchar(255)
);

create table sec_role(
 id int(10) primary key auto_increment,
 rolename varchar(100),
 prompt varchar(255)
);

create table sec_role_user(
 id int(10) primary key auto_increment,
 user_id int(10) not null,
 role_id int(10) not null
);

alter table sec_role_user add constraint fk_user foreign key(user_id) references sec_user(id);
alter table sec_role_user add constraint fk_role foreign key(role_id) references sec_role(id);