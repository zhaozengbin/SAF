package com.saf.security.h2.entity;


import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity(name = "sec_role")
public class RoleEntity {

    @Id
    @GeneratedValue
    private Long id;

    private String rolename;

    private String prompt;

    public RoleEntity() {
    }

    public RoleEntity(String rolename, String prompt) {
        this.rolename = rolename;
        this.prompt = prompt;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getRolename() {
        return rolename;
    }

    public void setRolename(String rolename) {
        this.rolename = rolename;
    }

    public String getPrompt() {
        return prompt;
    }

    public void setPrompt(String prompt) {
        this.prompt = prompt;
    }
}
