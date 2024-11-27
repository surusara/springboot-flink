package com.example.FlinkNettingApplication;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/netting-results")
public class NettingResultController {

    @Autowired
    private NettingResultRepository repository;

    @GetMapping
    public List<NettingResult> getAllNettingResults() {
        return repository.findAll();
    }
}
