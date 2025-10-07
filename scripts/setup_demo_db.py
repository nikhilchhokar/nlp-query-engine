"""
Demo Database Setup Script
Creates a sample employee database for testing the NLP Query Engine
"""

import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Date, Float, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime, timedelta
import random

Base = declarative_base()

# Define Models
class Department(Base):
    __tablename__ = 'departments'
    
    dept_id = Column(Integer, primary_key=True)
    dept_name = Column(String(100), nullable=False)
    manager_id = Column(Integer, ForeignKey('employees.emp_id'), nullable=True)
    budget = Column(Float, nullable=True)
    location = Column(String(100), nullable=True)
    
    employees = relationship("Employee", back_populates="department", foreign_keys="Employee.dept_id")

class Employee(Base):
    __tablename__ = 'employees'
    
    emp_id = Column(Integer, primary_key=True)
    full_name = Column(String(100), nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    dept_id = Column(Integer, ForeignKey('departments.dept_id'), nullable=True)
    position = Column(String(100), nullable=False)
    annual_salary = Column(Float, nullable=False)
    join_date = Column(Date, nullable=False)
    office_location = Column(String(100), nullable=True)
    manager_id = Column(Integer, ForeignKey('employees.emp_id'), nullable=True)
    skills = Column(Text, nullable=True)
    
    department = relationship("Department", back_populates="employees", foreign_keys=[dept_id])
    performance_reviews = relationship("PerformanceReview", back_populates="employee")

class PerformanceReview(Base):
    __tablename__ = 'performance_reviews'
    
    review_id = Column(Integer, primary_key=True)
    emp_id = Column(Integer, ForeignKey('employees.emp_id'), nullable=False)
    review_date = Column(Date, nullable=False)
    rating = Column(Integer, nullable=False)  # 1-5 scale
    comments = Column(Text, nullable=True)
    reviewer_name = Column(String(100), nullable=True)
    
    employee = relationship("Employee", back_populates="performance_reviews")

class Project(Base):
    __tablename__ = 'projects'
    
    project_id = Column(Integer, primary_key=True)
    project_name = Column(String(200), nullable=False)
    department_id = Column(Integer, ForeignKey('departments.dept_id'))
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=True)
    budget = Column(Float, nullable=True)
    status = Column(String(50), nullable=False)

def create_demo_database(db_url="sqlite:///demo_employee.db"):
    """Create and populate demo database"""
    
    print(f"Creating demo database at: {db_url}")
    engine = create_engine(db_url, echo=False)
    
    # Drop existing tables and create new ones
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Create departments
        departments_data = [
            {"dept_id": 1, "dept_name": "Engineering", "budget": 5000000, "location": "San Francisco"},
            {"dept_id": 2, "dept_name": "Sales", "budget": 3000000, "location": "New York"},
            {"dept_id": 3, "dept_name": "Marketing", "budget": 2000000, "location": "Los Angeles"},
            {"dept_id": 4, "dept_name": "Human Resources", "budget": 1000000, "location": "Chicago"},
            {"dept_id": 5, "dept_name": "Finance", "budget": 1500000, "location": "Boston"},
            {"dept_id": 6, "dept_name": "Operations", "budget": 2500000, "location": "Austin"},
            {"dept_id": 7, "dept_name": "Product", "budget": 3500000, "location": "Seattle"},
            {"dept_id": 8, "dept_name": "Customer Support", "budget": 800000, "location": "Denver"}
        ]
        
        for dept_data in departments_data:
            dept = Department(**dept_data)
            session.add(dept)
        
        session.commit()
        print(f"✓ Created {len(departments_data)} departments")
        
        # Create employees
        first_names = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry", 
                       "Iris", "Jack", "Kate", "Leo", "Mary", "Nathan", "Olivia", "Paul",
                       "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier",
                       "Yara", "Zack", "Amy", "Ben", "Claire", "Dan"]
        
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
                      "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", 
                      "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee"]
        
        positions = {
            1: ["Software Engineer", "Senior Software Engineer", "Lead Engineer", "DevOps Engineer", 
                "Data Engineer", "ML Engineer", "Frontend Developer", "Backend Developer"],
            2: ["Sales Representative", "Account Executive", "Sales Manager", "Regional Sales Director"],
            3: ["Marketing Specialist", "Content Writer", "Social Media Manager", "Marketing Manager"],
            4: ["HR Specialist", "Recruiter", "HR Manager", "Benefits Coordinator"],
            5: ["Financial Analyst", "Accountant", "Finance Manager", "Controller"],
            6: ["Operations Manager", "Logistics Coordinator", "Operations Analyst"],
            7: ["Product Manager", "Product Designer", "UX Researcher", "Product Analyst"],
            8: ["Support Specialist", "Customer Success Manager", "Support Team Lead"]
        }
        
        skills_pool = {
            1: ["Python", "Java", "JavaScript", "React", "Node.js", "Docker", "Kubernetes", 
                "AWS", "PostgreSQL", "MongoDB", "Machine Learning", "TensorFlow", "Django", "Flask"],
            2: ["Salesforce", "CRM", "Negotiation", "B2B Sales", "Lead Generation"],
            3: ["SEO", "Content Marketing", "Google Analytics", "Social Media", "Copywriting"],
            4: ["Recruitment", "Employee Relations", "HRIS", "Performance Management"],
            5: ["Excel", "Financial Modeling", "QuickBooks", "SAP", "Budgeting"],
            6: ["Supply Chain", "Logistics", "Process Improvement", "Lean Six Sigma"],
            7: ["Product Strategy", "User Research", "Roadmap Planning", "Agile", "JIRA"],
            8: ["Customer Service", "Zendesk", "Troubleshooting", "Communication"]
        }
        
        employees = []
        emp_id = 1
        
        for dept_id in range(1, 9):
            # Number of employees per department
            num_employees = random.randint(25, 35)
            
            for _ in range(num_employees):
                first_name = random.choice(first_names)
                last_name = random.choice(last_names)
                full_name = f"{first_name} {last_name}"
                email = f"{first_name.lower()}.{last_name.lower()}{emp_id}@company.com"
                position = random.choice(positions[dept_id])
                
                # Salary based on position seniority
                base_salary = 60000
                if "Senior" in position or "Lead" in position:
                    base_salary = 100000
                elif "Manager" in position or "Director" in position:
                    base_salary = 120000
                
                salary = base_salary + random.randint(-10000, 30000)
                
                # Random join date within last 5 years
                days_ago = random.randint(0, 1825)
                join_date = datetime.now().date() - timedelta(days=days_ago)
                
                # Select random skills
                dept_skills = skills_pool[dept_id]
                num_skills = random.randint(3, 6)
                employee_skills = ", ".join(random.sample(dept_skills, min(num_skills, len(dept_skills))))
                
                emp = Employee(
                    emp_id=emp_id,
                    full_name=full_name,
                    email=email,
                    dept_id=dept_id,
                    position=position,
                    annual_salary=salary,
                    join_date=join_date,
                    office_location=departments_data[dept_id-1]["location"],
                    skills=employee_skills
                )
                
                employees.append(emp)
                session.add(emp)
                emp_id += 1
        
        session.commit()
        print(f"✓ Created {len(employees)} employees")
        
        # Create performance reviews
        review_comments = [
            "Excellent performance, consistently exceeds expectations.",
            "Strong contributor to team goals and projects.",
            "Shows great initiative and leadership qualities.",
            "Meets expectations, reliable team member.",
            "Needs improvement in communication and collaboration.",
            "Outstanding technical skills and problem-solving ability.",
            "Very collaborative, helps mentor junior team members.",
            "Delivers high-quality work on time."
        ]
        
        reviews = []
        review_id = 1
        
        for emp in employees:
            # Create 2-4 reviews per employee
            num_reviews = random.randint(2, 4)
            
            for i in range(num_reviews):
                # Review dates spread over employment period
                days_since_join = (datetime.now().date() - emp.join_date).days
                if days_since_join > 180:  # At least 6 months employed
                    review_days_ago = random.randint(0, days_since_join - 90)
                    review_date = datetime.now().date() - timedelta(days=review_days_ago)
                    
                    rating = random.choices([1, 2, 3, 4, 5], weights=[5, 10, 25, 35, 25])[0]
                    comments = random.choice(review_comments)
                    
                    review = PerformanceReview(
                        review_id=review_id,
                        emp_id=emp.emp_id,
                        review_date=review_date,
                        rating=rating,
                        comments=comments,
                        reviewer_name=f"{random.choice(first_names)} {random.choice(last_names)}"
                    )
                    
                    reviews.append(review)
                    session.add(review)
                    review_id += 1
        
        session.commit()
        print(f"✓ Created {len(reviews)} performance reviews")
        
        # Create projects
        project_names = [
            "Website Redesign", "Mobile App Launch", "Data Migration", "API Integration",
            "Customer Portal", "Analytics Dashboard", "Security Audit", "Cloud Migration",
            "Marketing Campaign Q4", "Product Launch Alpha", "Infrastructure Upgrade",
            "CRM Implementation", "Employee Training Program", "Cost Optimization Initiative"
        ]
        
        projects = []
        for i, project_name in enumerate(project_names, 1):
            dept_id = random.randint(1, 8)
            start_date = datetime.now().date() - timedelta(days=random.randint(30, 365))
            
            # 70% completed, 30% ongoing
            if random.random() < 0.7:
                end_date = start_date + timedelta(days=random.randint(60, 180))
                status = "Completed"
            else:
                end_date = None
                status = "In Progress"
            
            project = Project(
                project_id=i,
                project_name=project_name,
                department_id=dept_id,
                start_date=start_date,
                end_date=end_date,
                budget=random.randint(50000, 500000),
                status=status
            )
            
            projects.append(project)
            session.add(project)
        
        session.commit()
        print(f"✓ Created {len(projects)} projects")
        
        # Print summary statistics
        print("\n" + "="*50)
        print("DATABASE SUMMARY")
        print("="*50)
        print(f"Total Departments: {len(departments_data)}")
        print(f"Total Employees: {len(employees)}")
        print(f"Total Reviews: {len(reviews)}")
        print(f"Total Projects: {len(projects)}")
        print(f"\nAverage Salary: ${sum(e.annual_salary for e in employees) / len(employees):,.2f}")
        print(f"Salary Range: ${min(e.annual_salary for e in employees):,.2f} - ${max(e.annual_salary for e in employees):,.2f}")
        print("\nEmployees by Department:")
        for dept in departments_data:
            count = sum(1 for e in employees if e.dept_id == dept['dept_id'])
            print(f"  {dept['dept_name']}: {count}")
        
        print("\n✓ Demo database created successfully!")
        print(f"Connection string: {db_url}")
        
    except Exception as e:
        session.rollback()
        print(f"✗ Error creating database: {str(e)}")
        raise
    finally:
        session.close()

def create_postgres_demo(host="localhost", port=5432, user="nlp_user", 
                         password="nlp_password", database="employee_db"):
    """Create demo database in PostgreSQL"""
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    create_demo_database(connection_string)

def create_mysql_demo(host="localhost", port=3306, user="nlp_user", 
                      password="nlp_password", database="employee_db"):
    """Create demo database in MySQL"""
    connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    create_demo_database(connection_string)

if __name__ == "__main__":
    import sys
    
    print("NLP Query Engine - Demo Database Setup")
    print("="*50)
    
    if len(sys.argv) > 1:
        db_type = sys.argv[1].lower()
        
        if db_type == "postgres":
            print("Creating PostgreSQL demo database...")
            create_postgres_demo()
        elif db_type == "mysql":
            print("Creating MySQL demo database...")
            create_mysql_demo()
        else:
            print(f"Unknown database type: {db_type}")
            print("Usage: python setup_demo_db.py [postgres|mysql|sqlite]")
            sys.exit(1)
    else:
        print("Creating SQLite demo database...")
        create_demo_database()
    
    print("\n" + "="*50)
    print("Setup complete! You can now connect to the database.")
    print("="*50)