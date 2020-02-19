from config import db

class Visits(db.Model):
    __tablename__ = 'visits'
    id = db.Column(db.Integer(), primary_key=True)

    cluster_id = db.Column(db.Integer())
    start = db.Column(db.DateTime)
    end = db.Column(db.DateTime)
    
    
    @staticmethod
    def add_db(objs):
        db.session.bulk_save_objects(objs)
        db.session.commit()
        
        
if __name__ == '__main__':
    db.create_all()
    db.session.commit()