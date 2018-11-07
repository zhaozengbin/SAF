package com.saf.web.controller;

import org.springframework.session.Session;
import org.springframework.stereotype.Controller;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

@Controller
public class BaseController {

    @RequestMapping(value = "/{path}", method = RequestMethod.GET)
    public ModelAndView index(@PathVariable(value = "path") String path, HttpSession session) {
        ModelAndView mv = new ModelAndView(path);
        return mv;
    }


    @RequestMapping(value = "/mllib/als/{path}", method = RequestMethod.GET)
    public ModelAndView mllib(@PathVariable(value = "path") String path, HttpSession session) {
        ModelAndView mv = new ModelAndView("mllib/als/" + path);
        return mv;
    }

    @RequestMapping(value = "/common/{path}", method = RequestMethod.GET)
    public ModelAndView common(@PathVariable(value = "path") String path, HttpSession session) {
        ModelAndView mv = new ModelAndView("common/" + path);
        return mv;
    }

    private String extractPathFromPattern(final HttpServletRequest request) {
        String path = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
        String bestMatchPattern = (String) request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
        return new AntPathMatcher().extractPathWithinPattern(bestMatchPattern, path);
    }
}
