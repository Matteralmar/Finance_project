from django.db import models
from django.utils.timezone import now


class Currency(models.Model):
    CURRENCY_CHOICES = (
        (1, 'USD'),
        (2, 'EUR'),
    )
    SOURCE_CHOICES = (
        (1, 'PRIVATE_BANK'),
        (2, 'MONO_BANK'),
        (3, 'VKURSE'),
        (4, 'YAHOO'),
    )
    currency = models.PositiveSmallIntegerField(choices=CURRENCY_CHOICES)
    source = models.PositiveSmallIntegerField(choices=SOURCE_CHOICES)
    buy = models.DecimalField(max_digits=6, decimal_places=2)
    sale = models.DecimalField(max_digits=6, decimal_places=2)

    created = models.DateTimeField(auto_now_add=True)
    updated = models.DateTimeField(default=now)
