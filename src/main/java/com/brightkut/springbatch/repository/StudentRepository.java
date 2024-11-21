package com.brightkut.springbatch.repository;

import com.brightkut.springbatch.entity.Student;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StudentRepository extends JpaRepository<Student, Integer> {
}
