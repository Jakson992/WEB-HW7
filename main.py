from sqlalchemy import func, desc, and_, distinct, select

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_1():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    :return:
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_2():
    """
    Знайти студента із найвищим середнім балом з певного предмета.
    :return:
    """
    discipline_id = 2
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student) \
        .join(Discipline, Grade.discipline_id == Discipline.id) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Student.id) \
        .order_by(-func.avg(Grade.grade)).first()
    return result


def select_3():
    """
    Знайти середній бал у групах з певного предмета.
    :return:
    """
    discipline_id = 2
    result = session.query(Group.name, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student) \
        .join(Group) \
        .join(Discipline, Grade.discipline_id == Discipline.id) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Group.name).all()
    return result


def select_4():
    """
    Знайти середній бал на потоці (по всій таблиці оцінок).
    :return:
    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade')).first()
    return result


def select_5():
    """
    Знайти, які курси читає певний викладач.
    :param teacher_name: Певний викладач
    :return:
    """
    teacher_id = 2
    result = session.query(Discipline.name) \
        .select_from(Discipline).join(Teacher) \
        .filter(Teacher.id == teacher_id).all()
    return result


def select_6():
    """
    Знайти список студентів у певній групі.
    :return:
    """
    group_id = 2
    result = session.query(Student.fullname) \
        .select_from(Student).join(Group) \
        .filter(Group.id == group_id).all()
    return result


def select_7():
    """
    Знайти оцінки студентів в окремій групі з певного предмета.
    :return:
    """
    group_id = 2
    subject_id = 3
    result = session.query(Grade.grade) \
        .select_from(Grade).join(Student).join(Discipline) \
        .filter(Student.group_id == group_id, Discipline.id == subject_id).all()
    return result


def select_8():
    """
    Знайти середній бал, який ставить певний викладач зі своїх предметів.
    :return:
    """
    result = session.query(distinct(Teacher.fullname), func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Discipline) \
        .join(Teacher) \
        .where(Teacher.id == 3).group_by(Teacher.fullname).order_by(desc('avg_grade')).limit(5).all()
    return result




def select_9():
    """
    Знайти список курсів, які відвідує певний студент.
    :return: Список курсів, які відвідує студент.
    """
    student_id = 36

    result = session.query(Discipline.name) \
        .join(Grade, Discipline.id == Grade.discipline_id) \
        .join(Student, Student.id == Grade.student_id) \
        .filter(Student.id == student_id) \
        .distinct() \
        .all()

    return result


def select_10():
    """
    Список курсів, які певному студенту читає певний викладач.
    :return: Список курсів, які певному студенту читає певний викладач..
    """
    student_id = 2
    teacher_id = 3

    result = session.query(Discipline.name) \
        .join(Grade, Discipline.id == Grade.discipline_id) \
        .join(Student, Student.id == Grade.student_id) \
        .join(Teacher, Teacher.id == Discipline.teacher_id) \
        .filter(Student.id == student_id, Teacher.id == teacher_id) \
        .distinct() \
        .all()

    return result


def select_12():
    """
    Оцінки студентів у певній групі з певного предмета на останньому занятті.
    :return:
    """
    group_id = 2
    dis_id = 2

    subq = (select(Grade.date_of).join(Student).join(Group).where(
        and_(Grade.discipline_id == dis_id, Group.id == group_id)
    ).order_by(desc(Grade.date_of)).limit(1)).scalar_subquery()

    result = session.query(Student.fullname, Discipline.name, Group.name, Grade.grade, Grade.date_of) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group) \
        .filter(and_(Grade.discipline_id == dis_id, Group.id == group_id, Grade.date_of == subq)) \
        .order_by(desc(Grade.date_of)).all()
    return result


if __name__ == '__main__':
    # print(select_1())
    # print(select_2())
    # print(select_3())
    # print(select_4())
    # print(select_5())
    # print(select_6())
    # print(select_7())
    # print(select_8())
    # print(select_9())
    print(select_10())
    # print(select_12())
