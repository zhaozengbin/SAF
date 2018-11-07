package com.saf.security.h2.dao;

import com.saf.security.h2.entity.UserRoleEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface IUserRoleDao extends JpaRepository<UserRoleEntity, Long> {
}
