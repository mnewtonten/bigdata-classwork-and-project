use BDA_VQ;

select courseid, count(*)
  from quiz_course_close_assoc
  group by courseid;

select quizid, count(*)
  from quizzes INNER JOIN

--sub selects
--build your statements up in peices
--you can select from a select 
