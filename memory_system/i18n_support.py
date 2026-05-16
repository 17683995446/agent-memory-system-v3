#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
国际化支持模块 (Internationalization Support Module)

提供多语言资源管理、动态语言切换、本地化格式(日期/数字/货币)、
RTL语言支持、翻译工作流和文化适配功能。

Author: Agent Memory System Team
Version: 3.0.0
"""

import json
import logging
import os
import re
import threading
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum, auto
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)
from functools import wraps

# 配置日志
logger = logging.getLogger(__name__)


class TextDirection(Enum):
    """文本方向"""
    LTR = "ltr"  # 从左到右
    RTL = "rtl"  # 从右到左


@dataclass
class Locale:
    """区域设置"""
    code: str  # 语言代码，如 "zh_CN", "en_US"
    name: str  # 语言名称
    native_name: str  # 本地名称
    direction: TextDirection = TextDirection.LTR
    plural_forms: int = 2  # 复数形式数量
    
    def __post_init__(self):
        # 自动检测RTL语言
        rtl_languages = {'ar', 'he', 'fa', 'ur', 'yi', 'dv'}
        lang_code = self.code.split('_')[0].lower()
        if lang_code in rtl_languages:
            self.direction = TextDirection.RTL


@dataclass
class TranslationMessage:
    """翻译消息"""
    key: str
    value: str
    context: Optional[str] = None
    plural_forms: Optional[Dict[str, str]] = None
    
    def get(self, count: Optional[int] = None) -> str:
        """获取翻译值，支持复数"""
        if count is not None and self.plural_forms:
            # 简化复数处理
            if count == 1 and 'one' in self.plural_forms:
                return self.plural_forms['one']
            elif 'other' in self.plural_forms:
                return self.plural_forms['other']
        return self.value


class NumberFormatter:
    """数字格式化器"""
    
    # 不同地区的数字格式
    FORMATS = {
        'en_US': {'decimal_sep': '.', 'thousand_sep': ',', 'grouping': [3, 3, 3]},
        'de_DE': {'decimal_sep': ',', 'thousand_sep': '.', 'grouping': [3, 3, 3]},
        'fr_FR': {'decimal_sep': ',', 'thousand_sep': ' ', 'grouping': [3, 3, 3]},
        'zh_CN': {'decimal_sep': '.', 'thousand_sep': ',', 'grouping': [3, 3, 3]},
        'hi_IN': {'decimal_sep': '.', 'thousand_sep': ',', 'grouping': [3, 2, 2]},
    }
    
    def __init__(self, locale_code: str = 'en_US'):
        self.locale_code = locale_code
        self.format_config = self.FORMATS.get(locale_code, self.FORMATS['en_US'])
    
    def format_number(self, number: Union[int, float, Decimal], 
                      decimal_places: int = 2) -> str:
        """格式化数字"""
        if isinstance(number, (int, float)):
            number = Decimal(str(number))
        
        # 四舍五入
        quantize_str = '0.' + '0' * decimal_places
        number = number.quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)
        
        # 分离整数和小数部分
        sign, digits, exponent = number.as_tuple()
        
        # 构建整数部分
        int_part = ''.join(map(str, digits[:exponent if exponent > 0 else 0]))
        if not int_part:
            int_part = '0'
        
        # 添加千位分隔符
        int_part = self._add_thousand_separators(int_part)
        
        # 构建小数部分
        if decimal_places > 0:
            frac_part = ''.join(map(str, digits[exponent if exponent > 0 else 0:]))
            frac_part = frac_part.ljust(decimal_places, '0')[:decimal_places]
            return f"{int_part}{self.format_config['decimal_sep']}{frac_part}"
        
        return int_part
    
    def _add_thousand_separators(self, number_str: str) -> str:
        """添加千位分隔符"""
        if len(number_str) <= 3:
            return number_str
        
        result = []
        grouping = self.format_config['grouping']
        thousand_sep = self.format_config['thousand_sep']
        
        # 从右向左处理
        digits = list(number_str)
        group_idx = 0
        count = 0
        
        for i in range(len(digits) - 1, -1, -1):
            if count == grouping[min(group_idx, len(grouping) - 1)] and i > 0:
                result.append(thousand_sep)
                count = 0
                group_idx += 1
            result.append(digits[i])
            count += 1
        
        return ''.join(reversed(result))
    
    def format_percent(self, value: float, decimal_places: int = 1) -> str:
        """格式化百分比"""
        return self.format_number(value * 100, decimal_places) + '%'
    
    def parse_number(self, value: str) -> Decimal:
        """解析数字字符串"""
        # 移除千位分隔符
        value = value.replace(self.format_config['thousand_sep'], '')
        # 替换小数分隔符
        value = value.replace(self.format_config['decimal_sep'], '.')
        return Decimal(value)


class CurrencyFormatter:
    """货币格式化器"""
    
    # 货币配置
    CURRENCIES = {
        'USD': {'symbol': '$', 'position': 'before', 'decimal_places': 2},
        'EUR': {'symbol': '€', 'position': 'after', 'decimal_places': 2},
        'GBP': {'symbol': '£', 'position': 'before', 'decimal_places': 2},
        'CNY': {'symbol': '¥', 'position': 'before', 'decimal_places': 2},
        'JPY': {'symbol': '¥', 'position': 'before', 'decimal_places': 0},
        'INR': {'symbol': '₹', 'position': 'before', 'decimal_places': 2},
    }
    
    def __init__(self, locale_code: str = 'en_US', currency_code: str = 'USD'):
        self.locale_code = locale_code
        self.currency_code = currency_code
        self.number_formatter = NumberFormatter(locale_code)
        self.currency_config = self.CURRENCIES.get(currency_code, self.CURRENCIES['USD'])
    
    def format(self, amount: Union[int, float, Decimal]) -> str:
        """格式化货币"""
        formatted_number = self.number_formatter.format_number(
            amount, 
            self.currency_config['decimal_places']
        )
        
        symbol = self.currency_config['symbol']
        if self.currency_config['position'] == 'before':
            return f"{symbol}{formatted_number}"
        else:
            return f"{formatted_number} {symbol}"
    
    def format_with_code(self, amount: Union[int, float, Decimal]) -> str:
        """格式化货币（带代码）"""
        formatted = self.format(amount)
        return f"{formatted} {self.currency_code}"


class DateTimeFormatter:
    """日期时间格式化器"""
    
    # 日期格式模板
    FORMATS = {
        'en_US': {
            'short_date': '%m/%d/%Y',
            'long_date': '%B %d, %Y',
            'short_datetime': '%m/%d/%Y %I:%M %p',
            'long_datetime': '%B %d, %Y at %I:%M:%S %p',
            'time': '%I:%M %p',
        },
        'zh_CN': {
            'short_date': '%Y-%m-%d',
            'long_date': '%Y年%m月%d日',
            'short_datetime': '%Y-%m-%d %H:%M',
            'long_datetime': '%Y年%m月%d日 %H:%M:%S',
            'time': '%H:%M',
        },
        'de_DE': {
            'short_date': '%d.%m.%Y',
            'long_date': '%d. %B %Y',
            'short_datetime': '%d.%m.%Y %H:%M',
            'long_datetime': '%d. %B %Y %H:%M:%S',
            'time': '%H:%M',
        },
        'ja_JP': {
            'short_date': '%Y/%m/%d',
            'long_date': '%Y年%m月%d日',
            'short_datetime': '%Y/%m/%d %H:%M',
            'long_datetime': '%Y年%m月%d日 %H:%M:%S',
            'time': '%H:%M',
        },
    }
    
    # 月份名称
    MONTH_NAMES = {
        'en_US': ['January', 'February', 'March', 'April', 'May', 'June',
                  'July', 'August', 'September', 'October', 'November', 'December'],
        'zh_CN': ['一月', '二月', '三月', '四月', '五月', '六月',
                  '七月', '八月', '九月', '十月', '十一月', '十二月'],
    }
    
    def __init__(self, locale_code: str = 'en_US'):
        self.locale_code = locale_code
        self.formats = self.FORMATS.get(locale_code, self.FORMATS['en_US'])
    
    def format_date(self, value: Union[datetime, date], format_type: str = 'short_date') -> str:
        """格式化日期"""
        if isinstance(value, date) and not isinstance(value, datetime):
            value = datetime.combine(value, datetime.min.time())
        
        format_str = self.formats.get(format_type, self.formats['short_date'])
        return value.strftime(format_str)
    
    def format_datetime(self, value: datetime, format_type: str = 'short_datetime') -> str:
        """格式化日期时间"""
        format_str = self.formats.get(format_type, self.formats['short_datetime'])
        return value.strftime(format_str)
    
    def format_relative(self, value: datetime, now: Optional[datetime] = None) -> str:
        """格式化相对时间"""
        if now is None:
            now = datetime.utcnow()
        
        diff = now - value
        seconds = diff.total_seconds()
        
        if seconds < 60:
            return self._get_translation('just_now')
        elif seconds < 3600:
            minutes = int(seconds / 60)
            return self._get_translation('minutes_ago', count=minutes)
        elif seconds < 86400:
            hours = int(seconds / 3600)
            return self._get_translation('hours_ago', count=hours)
        elif seconds < 604800:
            days = int(seconds / 86400)
            return self._get_translation('days_ago', count=days)
        else:
            return self.format_date(value)
    
    def _get_translation(self, key: str, count: Optional[int] = None) -> str:
        """获取翻译（简化实现）"""
        translations = {
            'en_US': {
                'just_now': 'just now',
                'minutes_ago': '{count} minute(s) ago',
                'hours_ago': '{count} hour(s) ago',
                'days_ago': '{count} day(s) ago',
            },
            'zh_CN': {
                'just_now': '刚刚',
                'minutes_ago': '{count}分钟前',
                'hours_ago': '{count}小时前',
                'days_ago': '{count}天前',
            },
        }
        
        locale_trans = translations.get(self.locale_code, translations['en_US'])
        text = locale_trans.get(key, key)
        
        if count is not None:
            text = text.replace('{count}', str(count))
            # 中文不需要复数形式
            if self.locale_code == 'zh_CN':
                text = text.replace('(s)', '')
            else:
                text = text.replace('(s)', 's' if count != 1 else '')
        
        return text


class TranslationStore:
    """翻译存储"""
    
    def __init__(self):
        self._translations: Dict[str, Dict[str, TranslationMessage]] = defaultdict(dict)
        self._lock = threading.Lock()
    
    def load_from_file(self, file_path: str, locale_code: str) -> bool:
        """从文件加载翻译"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            with self._lock:
                for key, value in data.items():
                    if isinstance(value, str):
                        self._translations[locale_code][key] = TranslationMessage(key, value)
                    elif isinstance(value, dict):
                        plural_forms = {k: v for k, v in value.items() if k != 'default'}
                        default_value = value.get('default', value.get('other', ''))
                        self._translations[locale_code][key] = TranslationMessage(
                            key, default_value, plural_forms=plural_forms
                        )
            
            return True
        except Exception as e:
            logger.error(f"Failed to load translations from {file_path}: {e}")
            return False
    
    def load_from_directory(self, directory: str) -> int:
        """从目录加载所有翻译文件"""
        count = 0
        
        if not os.path.exists(directory):
            return 0
        
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                locale_code = filename[:-5]  # 移除 .json
                file_path = os.path.join(directory, filename)
                if self.load_from_file(file_path, locale_code):
                    count += 1
        
        return count
    
    def get(self, key: str, locale_code: str, default: Optional[str] = None) -> str:
        """获取翻译"""
        with self._lock:
            message = self._translations.get(locale_code, {}).get(key)
            if message:
                return message.value
            
            # 尝试基础语言代码
            base_code = locale_code.split('_')[0]
            if base_code != locale_code:
                message = self._translations.get(base_code, {}).get(key)
                if message:
                    return message.value
        
        return default or key
    
    def get_with_plural(self, key: str, locale_code: str, count: int) -> str:
        """获取带复数形式的翻译"""
        with self._lock:
            message = self._translations.get(locale_code, {}).get(key)
            if message:
                return message.get(count)
        
        return key
    
    def add_translation(self, locale_code: str, key: str, value: str,
                        plural_forms: Optional[Dict[str, str]] = None) -> None:
        """添加翻译"""
        with self._lock:
            self._translations[locale_code][key] = TranslationMessage(
                key, value, plural_forms=plural_forms
            )
    
    def get_locale_coverage(self, locale_code: str) -> Dict[str, Any]:
        """获取翻译覆盖率"""
        with self._lock:
            locale_keys = set(self._translations.get(locale_code, {}).keys())
            
            # 以英语为基准
            base_keys = set(self._translations.get('en', {}).keys())
            if not base_keys:
                base_keys = set(self._translations.get('en_US', {}).keys())
            
            if not base_keys:
                return {'coverage': 0, 'total': 0, 'translated': 0}
            
            translated = len(locale_keys & base_keys)
            total = len(base_keys)
            
            return {
                'coverage': translated / total if total > 0 else 0,
                'total': total,
                'translated': translated,
                'missing': list(base_keys - locale_keys),
            }


class I18nManager:
    """国际化管理器"""
    
    def __init__(self, default_locale: str = 'en_US'):
        self.default_locale = default_locale
        self.current_locale = default_locale
        self._locale_stack: List[str] = []
        
        self.translation_store = TranslationStore()
        self.number_formatter: Optional[NumberFormatter] = None
        self.currency_formatter: Optional[CurrencyFormatter] = None
        self.datetime_formatter: Optional[DateTimeFormatter] = None
        
        self._lock = threading.Lock()
        self._listeners: List[Callable[[str], None]] = []
        
        # 初始化默认格式化器
        self._update_formatters()
    
    def _update_formatters(self) -> None:
        """更新格式化器"""
        self.number_formatter = NumberFormatter(self.current_locale)
        self.datetime_formatter = DateTimeFormatter(self.current_locale)
    
    def set_locale(self, locale_code: str) -> None:
        """设置当前语言"""
        with self._lock:
            old_locale = self.current_locale
            self.current_locale = locale_code
            self._update_formatters()
            
            # 通知监听器
            for listener in self._listeners:
                try:
                    listener(locale_code)
                except Exception as e:
                    logger.error(f"Locale change listener failed: {e}")
    
    def get_locale(self) -> str:
        """获取当前语言"""
        with self._lock:
            return self.current_locale
    
    def push_locale(self, locale_code: str) -> None:
        """压入语言到栈"""
        with self._lock:
            self._locale_stack.append(self.current_locale)
            self.set_locale(locale_code)
    
    def pop_locale(self) -> Optional[str]:
        """从栈弹出语言"""
        with self._lock:
            if self._locale_stack:
                locale_code = self._locale_stack.pop()
                self.set_locale(locale_code)
                return locale_code
            return None
    
    def add_locale_change_listener(self, listener: Callable[[str], None]) -> None:
        """添加语言变更监听器"""
        self._listeners.append(listener)
    
    def translate(self, key: str, **kwargs) -> str:
        """翻译"""
        with self._lock:
            locale_code = self.current_locale
        
        value = self.translation_store.get(key, locale_code, key)
        
        # 替换变量
        if kwargs:
            for var_name, var_value in kwargs.items():
                value = value.replace(f'{{{var_name}}}', str(var_value))
        
        return value
    
    def translate_plural(self, key: str, count: int, **kwargs) -> str:
        """翻译（带复数）"""
        with self._lock:
            locale_code = self.current_locale
        
        value = self.translation_store.get_with_plural(key, locale_code, count)
        
        # 替换变量
        if kwargs:
            for var_name, var_value in kwargs.items():
                value = value.replace(f'{{{var_name}}}', str(var_value))
        
        return value
    
    def format_number(self, number: Union[int, float, Decimal], 
                      decimal_places: int = 2) -> str:
        """格式化数字"""
        if self.number_formatter is None:
            self._update_formatters()
        return self.number_formatter.format_number(number, decimal_places)
    
    def format_currency(self, amount: Union[int, float, Decimal], 
                        currency_code: Optional[str] = None) -> str:
        """格式化货币"""
        if currency_code:
            formatter = CurrencyFormatter(self.current_locale, currency_code)
        else:
            if self.currency_formatter is None:
                self.currency_formatter = CurrencyFormatter(self.current_locale)
            formatter = self.currency_formatter
        
        return formatter.format(amount)
    
    def format_date(self, value: Union[datetime, date], 
                    format_type: str = 'short_date') -> str:
        """格式化日期"""
        if self.datetime_formatter is None:
            self._update_formatters()
        return self.datetime_formatter.format_date(value, format_type)
    
    def format_datetime(self, value: datetime, 
                        format_type: str = 'short_datetime') -> str:
        """格式化日期时间"""
        if self.datetime_formatter is None:
            self._update_formatters()
        return self.datetime_formatter.format_datetime(value, format_type)
    
    def format_relative_time(self, value: datetime) -> str:
        """格式化相对时间"""
        if self.datetime_formatter is None:
            self._update_formatters()
        return self.datetime_formatter.format_relative(value)
    
    def get_text_direction(self) -> TextDirection:
        """获取文本方向"""
        # 简化实现
        rtl_languages = {'ar', 'he', 'fa', 'ur'}
        lang_code = self.current_locale.split('_')[0].lower()
        return TextDirection.RTL if lang_code in rtl_languages else TextDirection.LTR
    
    def is_rtl(self) -> bool:
        """是否为RTL语言"""
        return self.get_text_direction() == TextDirection.RTL


class CultureAdapter:
    """文化适配器"""
    
    # 文化特定的问候语
    GREETINGS = {
        'en_US': {'formal': 'Hello', 'informal': 'Hi'},
        'zh_CN': {'formal': '您好', 'informal': '你好'},
        'ja_JP': {'formal': 'こんにちは', 'informal': 'やあ'},
        'de_DE': {'formal': 'Guten Tag', 'informal': 'Hallo'},
        'fr_FR': {'formal': 'Bonjour', 'informal': 'Salut'},
        'es_ES': {'formal': 'Hola', 'informal': '¡Hola!'},
        'ar_SA': {'formal': 'السلام عليكم', 'informal': 'مرحبا'},
    }
    
    # 姓名格式
    NAME_FORMATS = {
        'en_US': '{first_name} {last_name}',
        'zh_CN': '{last_name}{first_name}',
        'ja_JP': '{last_name} {first_name}',
        'ko_KR': '{last_name}{first_name}',
    }
    
    @classmethod
    def get_greeting(cls, locale_code: str, formal: bool = True) -> str:
        """获取问候语"""
        greetings = cls.GREETINGS.get(locale_code, cls.GREETINGS['en_US'])
        return greetings['formal'] if formal else greetings['informal']
    
    @classmethod
    def format_name(cls, first_name: str, last_name: str, locale_code: str) -> str:
        """格式化姓名"""
        format_str = cls.NAME_FORMATS.get(locale_code, cls.NAME_FORMATS['en_US'])
        return format_str.format(first_name=first_name, last_name=last_name)
    
    @classmethod
    def get_address_format(cls, locale_code: str) -> str:
        """获取地址格式"""
        # 简化实现
        if locale_code in ['zh_CN', 'ja_JP', 'ko_KR']:
            return '{country} {province} {city} {district} {street}'
        return '{street}, {city}, {province} {postal_code}, {country}'


class I18nSupport:
    """国际化支持主类"""
    
    def __init__(self, default_locale: str = 'en_US'):
        self.manager = I18nManager(default_locale)
        self.supported_locales: Dict[str, Locale] = {}
        
        # 注册默认支持的语言
        self._register_default_locales()
    
    def _register_default_locales(self) -> None:
        """注册默认支持的语言"""
        locales = [
            Locale('en_US', 'English (US)', 'English'),
            Locale('zh_CN', 'Chinese (Simplified)', '简体中文'),
            Locale('zh_TW', 'Chinese (Traditional)', '繁體中文'),
            Locale('ja_JP', 'Japanese', '日本語'),
            Locale('ko_KR', 'Korean', '한국어'),
            Locale('de_DE', 'German', 'Deutsch'),
            Locale('fr_FR', 'French', 'Français'),
            Locale('es_ES', 'Spanish', 'Español'),
            Locale('it_IT', 'Italian', 'Italiano'),
            Locale('pt_BR', 'Portuguese (Brazil)', 'Português'),
            Locale('ru_RU', 'Russian', 'Русский'),
            Locale('ar_SA', 'Arabic', 'العربية', TextDirection.RTL),
            Locale('he_IL', 'Hebrew', 'עברית', TextDirection.RTL),
            Locale('hi_IN', 'Hindi', 'हिन्दी'),
            Locale('th_TH', 'Thai', 'ไทย'),
        ]
        
        for locale in locales:
            self.supported_locales[locale.code] = locale
    
    def load_translations(self, directory: str) -> int:
        """加载翻译文件"""
        return self.manager.translation_store.load_from_directory(directory)
    
    def add_translation(self, locale_code: str, key: str, value: str,
                        plural_forms: Optional[Dict[str, str]] = None) -> None:
        """添加翻译"""
        self.manager.translation_store.add_translation(locale_code, key, value, plural_forms)
    
    def switch_locale(self, locale_code: str) -> bool:
        """切换语言"""
        if locale_code not in self.supported_locales:
            logger.warning(f"Unsupported locale: {locale_code}")
            return False
        
        self.manager.set_locale(locale_code)
        return True
    
    def get_supported_locales(self) -> List[Locale]:
        """获取支持的语言列表"""
        return list(self.supported_locales.values())
    
    def get_translation_coverage(self, locale_code: str) -> Dict[str, Any]:
        """获取翻译覆盖率"""
        return self.manager.translation_store.get_locale_coverage(locale_code)
    
    def t(self, key: str, **kwargs) -> str:
        """快捷翻译方法"""
        return self.manager.translate(key, **kwargs)
    
    def tp(self, key: str, count: int, **kwargs) -> str:
        """快捷复数翻译方法"""
        return self.manager.translate_plural(key, count, **kwargs)
    
    def get_locale_info(self) -> Dict[str, Any]:
        """获取当前语言信息"""
        locale_code = self.manager.get_locale()
        locale = self.supported_locales.get(locale_code)
        
        return {
            'code': locale_code,
            'name': locale.name if locale else locale_code,
            'native_name': locale.native_name if locale else locale_code,
            'direction': locale.direction.value if locale else 'ltr',
            'is_rtl': self.manager.is_rtl(),
        }


# 全局国际化支持实例
_i18n_support: Optional[I18nSupport] = None


def get_i18n_support(default_locale: str = 'en_US') -> I18nSupport:
    """获取全局国际化支持实例"""
    global _i18n_support
    if _i18n_support is None:
        _i18n_support = I18nSupport(default_locale)
    return _i18n_support


# 快捷函数
def _(key: str, **kwargs) -> str:
    """翻译快捷函数"""
    return get_i18n_support().t(key, **kwargs)


def _n(key: str, count: int, **kwargs) -> str:
    """复数翻译快捷函数"""
    return get_i18n_support().tp(key, count, **kwargs)


if __name__ == "__main__":
    # 示例用法
    logging.basicConfig(level=logging.INFO)
    
    print("=== I18n Support Demo ===\n")
    
    # 创建国际化支持
    i18n = get_i18n_support('en_US')
    
    # 添加一些翻译
    i18n.add_translation('en_US', 'hello', 'Hello, {name}!')
    i18n.add_translation('en_US', 'items_count', 'You have {count} item(s)', {
        'one': 'You have {count} item',
        'other': 'You have {count} items'
    })
    
    i18n.add_translation('zh_CN', 'hello', '你好，{name}！')
    i18n.add_translation('zh_CN', 'items_count', '你有 {count} 个项目')
    
    i18n.add_translation('ja_JP', 'hello', 'こんにちは、{name}さん！')
    
    # 示例1: 基本翻译
    print("1. Basic Translation:")
    print(f"   EN: {i18n.t('hello', name='World')}")
    
    i18n.switch_locale('zh_CN')
    print(f"   CN: {i18n.t('hello', name='世界')}")
    
    i18n.switch_locale('ja_JP')
    print(f"   JP: {i18n.t('hello', name='World')}")
    
    # 示例2: 复数翻译
    print("\n2. Plural Translation:")
    i18n.switch_locale('en_US')
    print(f"   1 item: {i18n.tp('items_count', 1, count=1)}")
    print(f"   5 items: {i18n.tp('items_count', 5, count=5)}")
    
    # 示例3: 数字格式化
    print("\n3. Number Formatting:")
    
    i18n.switch_locale('en_US')
    print(f"   US: {i18n.manager.format_number(1234567.89)}")
    
    i18n.switch_locale('de_DE')
    print(f"   DE: {i18n.manager.format_number(1234567.89)}")
    
    i18n.switch_locale('fr_FR')
    print(f"   FR: {i18n.manager.format_number(1234567.89)}")
    
    # 示例4: 货币格式化
    print("\n4. Currency Formatting:")
    
    i18n.switch_locale('en_US')
    print(f"   USD: {i18n.manager.format_currency(1234.56, 'USD')}")
    
    i18n.switch_locale('de_DE')
    print(f"   EUR: {i18n.manager.format_currency(1234.56, 'EUR')}")
    
    i18n.switch_locale('zh_CN')
    print(f"   CNY: {i18n.manager.format_currency(1234.56, 'CNY')}")
    
    # 示例5: 日期格式化
    print("\n5. Date Formatting:")
    
    now = datetime(2024, 1, 15, 14, 30, 0)
    
    i18n.switch_locale('en_US')
    print(f"   US: {i18n.manager.format_date(now, 'long_date')}")
    
    i18n.switch_locale('zh_CN')
    print(f"   CN: {i18n.manager.format_date(now, 'long_date')}")
    
    i18n.switch_locale('de_DE')
    print(f"   DE: {i18n.manager.format_date(now, 'long_date')}")
    
    # 示例6: RTL支持
    print("\n6. RTL Language Support:")
    
    for locale_code in ['en_US', 'ar_SA', 'he_IL']:
        i18n.switch_locale(locale_code)
        info = i18n.get_locale_info()
        print(f"   {locale_code}: direction={info['direction']}, is_rtl={info['is_rtl']}")
    
    # 示例7: 文化适配
    print("\n7. Culture Adaptation:")
    
    print(f"   US greeting: {CultureAdapter.get_greeting('en_US')}")
    print(f"   CN greeting: {CultureAdapter.get_greeting('zh_CN')}")
    print(f"   JP greeting: {CultureAdapter.get_greeting('ja_JP')}")
    
    print(f"   US name: {CultureAdapter.format_name('John', 'Doe', 'en_US')}")
    print(f"   CN name: {CultureAdapter.format_name('明', '李', 'zh_CN')}")
    
    # 示例8: 支持的语言
    print("\n8. Supported Locales:")
    
    locales = i18n.get_supported_locales()
    print(f"   Total: {len(locales)} locales")
    for locale in locales[:5]:
        print(f"   - {locale.code}: {locale.name} ({locale.native_name})")
    print(f"   ... and {len(locales) - 5} more")
    
    print("\n=== I18n Support Demo Completed ===")
