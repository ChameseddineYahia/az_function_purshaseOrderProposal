from marshmallow import Schema, fields, validate
from marshmallow.exceptions import ValidationError



class StringOrIntegerField(fields.Field):
    def _deserialize(self, value, attr, data, **kwargs):

        if not isinstance(value, (float, str,int)):
            raise ValidationError(f"{self.name} must be either a string or an integer or a float.")
        return value
    def _validate(self, value):
        if isinstance(value, str):
            super()._validate(value)

class XXAIH_POS_SO_IMPORTSchema(Schema):
    OUID = fields.Integer(allow_none=False)
    ORGID = fields.Integer(allow_none=False)
    REQ_ID = StringOrIntegerField(allow_none=False, validate=validate.Length(max=100))
    REQ_DATE = StringOrIntegerField(allow_none=False)
    REQ_USER = StringOrIntegerField(allow_none=True, validate=validate.Length(max=50))
    SUP_CODE = StringOrIntegerField(allow_none=False, validate=validate.Length(max=50))
    SUP_SITE = StringOrIntegerField(allow_none=False, validate=validate.Length(max=100))
    REQ_LINE_NO = fields.Integer(allow_none=False)
    ITEM_ID = fields.Integer(allow_none=False)
    ITEM_CODE = StringOrIntegerField(allow_none=False, validate=validate.Length(max=20))
    UOM_CODE = StringOrIntegerField(allow_none=False, validate=validate.Length(max=20))
    QUANTITY = fields.Integer(allow_none=False)
    ERPFLAG = fields.Integer(allow_none=False)
    ERRORMSG = StringOrIntegerField(allow_none=True, validate=validate.Length(max=4000))
    ADDDATE = StringOrIntegerField(allow_none=True)
    TOTAL_QUANTITY = fields.Integer(allow_none=False)