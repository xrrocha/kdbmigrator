drop table if exists emp;
drop table if exists dept;

create table dept
(
    deptno          integer     not null,
    dname           varchar(14) not null,
    loc             varchar(13) null,
    creation_date   date,
    primary key (deptno)
);

create table emp
(
    empno           integer       not null,
    ename           varchar(10)   not null,
    job             varchar(9)    not null,
    mgr             integer,
    hiredate        date          not null,
    sal             numeric(7, 2) not null,
    comm            numeric(7, 2),
    deptno          integer       not null,
    creation_date   date,
    primary key (empno),
    foreign key (mgr) references emp (empno),
    foreign key (deptno) references dept (deptno)
);
