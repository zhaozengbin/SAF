package com.saf.security.h2.dao;

import com.saf.security.h2.entity.RoleEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface IRoleDao extends JpaRepository<RoleEntity, Long> {
}
