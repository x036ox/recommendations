package com.artur.recommendations.controller;

import com.artur.common.exception.NotFoundException;
import com.artur.recommendations.service.RecommendationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/recs")
public class RecommendationsController {

    @Autowired
    RecommendationService recommendationService;

    @GetMapping("")
    public ResponseEntity<?> getRecommendations(
            @RequestParam(required = false) String userId,
            @RequestParam(required = false, defaultValue = "0") Integer page,
            @RequestParam String languages,
            @RequestParam Integer size
    ){
        try {
            return ResponseEntity.ok(recommendationService.getRecommendationsFor(
                    userId,
                    page,
                    languages.split(","),
                    size));
        } catch (NotFoundException e){
            return ResponseEntity.notFound().build();
        }
    }
}
