use BDA_VQ;

--question 1
select courseid, count(*)
  from quiz_course_close_assoc
  group by courseid;

--question 2
select quizid, count(distinct question_id) from code_answers group by quizid;
select quizid, count(mc_question_id) from multiple_choice_assoc group by quizid;
select x.quizid, (x.one+y.two) from 
  (select quizid, count(distinct question_id one 
    from code_answers group by quizid) x 
  INNER JOIN 
  (select quizid, count(mc_questions_id) two
    from multiple_choice_assoc group by quizid) y
  WHERE x.quizid = y.quizid;

--question 3
select count(distinct userid) from code_answers where question_type = 1;
select count(distinct userid) from code_answers where question_type = 1 AND correct = 1;

--question 4
select distinct quizid from lambda_assoc;
select distinct courseid from quiz_course_close_assoc where quizid = 11;

--question 5
select count(*), code_answers.userid 
  from code_answers, mc_answers 
  where (mc_answers.correct = 1 AND code_answers.correct = 1 
  AND code_answers.userid = mc_answers.userid)
  group by code_answers.userid;

--question 6
select count(*), spec_type from variable_specifications group by spec_type;

--question 7
select distinct quizid from code_answers; --21
select distinct quizid from quizzes; --55

--question 8 
select avg(T.average) from (select quizid, count(*) average from multiple_choice_assoc group by quizid) T;

--question 9
select avg(T.average) from (select quizid, count(distinct question_id) average from code_answers group by quizid) T;

--sub selects
--build your statements up in peices
--you can select from a select 
