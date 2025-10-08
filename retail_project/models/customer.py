class Customer:
    def __init__(self,ID=None,Name=None,Phone=None,Email=None,Password=None,IsDeleted=None):
        self.ID=ID
        self.Name=Name
        self.Phone=Phone
        self.Email=Email
        self.Password=Password
        self.IsDeleted=IsDeleted
    def __str__(self):
        info="{}\t{}\t{}\t{}\t{}\t{}".format(self.ID,self.Name,self.Phone,self.Email,self.Password,self.IsDeleted)
        return info