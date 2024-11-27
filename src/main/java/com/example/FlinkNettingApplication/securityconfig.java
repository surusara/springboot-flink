package com.example.FlinkNettingApplication;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .authorizeHttpRequests(auth -> auth.anyRequest().permitAll()) // Updated method for Spring Security 6.x+
                .csrf(csrf -> csrf.disable()); // Disable CSRF

        return http.build();
    }
}
