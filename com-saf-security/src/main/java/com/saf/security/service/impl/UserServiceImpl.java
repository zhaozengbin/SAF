package com.saf.security.service.impl;

import com.saf.security.h2.dao.IUserDao;
import com.saf.security.h2.entity.UserEntity;
import com.saf.security.service.IUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Example;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserServiceImpl implements IUserService {

    @Autowired
    private IUserDao iUserDao;

    @Override
    public boolean login(String username, String password) {
        List<UserEntity> userEntityList = iUserDao.findAll();
        Example<UserEntity> example = Example.of(new UserEntity(username, password));
        return iUserDao.exists(example);
    }
}
