package com.brightkut.springbatch.config;

import com.brightkut.springbatch.entity.Student;
import org.springframework.batch.item.ItemProcessor;

public class StudentProcessor implements ItemProcessor<Student,Student> {

    @Override
    public Student process(Student student) throws Exception {
        // all business logic here
        student.setId(null);
        return student;
    }

}
