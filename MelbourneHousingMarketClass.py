
class MHMC:
    def __init__(self,
                 header_dict=None,
                 id=None,
                 suberb=None,
                 adress=None,
                 rooms=None,
                 type=None,
                 price=None,
                 method=None,
                 sellerG=None,
                 date=None,
                 distance=None,
                 Postcode=None,
                 Bedroom2=None,
                 Bathroom=None,
                 Car=None,
                 landsize=None,
                 buildingarea=None,
                 yearbuilt=None,
                 councilArea=None,
                 lattitude=None,
                 longtitude=None,
                 regioname=None,
                 propercount=None):

        self.id = id
        self.suberb = suberb
        self.adress = adress
        self.rooms = rooms
        self.type = type
        self.price = price
        self.method = method
        self.sellerG = sellerG
        self.date = date
        self.distance = distance
        self.postcode = Postcode
        self.bedrooom2 = Bedroom2
        self.bathroom = Bathroom
        self.carspot = Car
        self.landsize = landsize
        self.buildingarea = buildingarea
        self.yearbuilt = yearbuilt
        self.councilArea = councilArea
        self.lattitude = lattitude
        self.longitude = longtitude
        self.regioname = regioname
        self.propercount = propercount

        self.json = {header_dict[0]: str(self.id),
                     header_dict[1]: str(self.suberb),
                     header_dict[2]: str(self.adress),
                     header_dict[3]: str(self.rooms),
                     header_dict[4]: str(self.type),
                     header_dict[5]: str(self.price),
                     header_dict[6]: str(self.method),
                     header_dict[7]: str(self.sellerG),
                     header_dict[8]: str(self.date),
                     header_dict[9]: str(self.distance),
                     header_dict[10]: str(self.postcode),
                     header_dict[11]: str(self.bedrooom2),
                     header_dict[12]: str(self.bathroom),
                     header_dict[13]: str(self.carspot),
                     header_dict[14]: str(self.landsize),
                     header_dict[15]: str(self.buildingarea),
                     header_dict[16]: str(self.yearbuilt),
                     header_dict[17]: str(self.councilArea),
                     header_dict[18]: str(self.lattitude),
                     header_dict[19]: str(self.longitude),
                     header_dict[20]: str(self.regioname),
                     header_dict[21]: str(self.propercount)
                     }