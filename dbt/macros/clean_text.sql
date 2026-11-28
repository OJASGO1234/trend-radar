{# clean_text(col): the YouTube API returns HTML-escaped text (&quot; &amp; &#39; ...).
   1) turn the common entities back into characters  2) collapse every run of whitespace
   (spaces, tabs, newlines) into one space  3) trim the ends. #}
{% macro clean_text(col) -%}
btrim(regexp_replace(
    replace(replace(replace(replace(replace({{ col }},
        '&quot;', '"'), '&#39;', ''''), '&lt;', '<'), '&gt;', '>'), '&amp;', '&'),
    '\s+', ' ', 'g'))
{%- endmacro %}
