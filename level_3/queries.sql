-- ============================================================================
-- Question 1: Oncology Specialization Rate
-- Calculate the ratio of applications for oncology trials to the total number
-- of applications for each Academic site.
-- ============================================================================

SELECT
    site_name,
    COUNT(CASE WHEN LOWER(therapeutic_area) = 'oncology' THEN 1 END) AS oncology_applications,
    COUNT(*) AS total_applications,
    ROUND(
        COUNT(CASE WHEN LOWER(therapeutic_area) = 'oncology' THEN 1 END) * 1.0 / COUNT(*),
        4
    ) AS oncology_specialization_rate
FROM application
WHERE LOWER(site_category) = 'academic'
GROUP BY site_name
ORDER BY oncology_specialization_rate DESC, site_name ASC;


-- ============================================================================
-- Question 2: High Velocity Sites
-- Provide a list of sites that applied to at least 10 trials during the 14 days
-- following their first application (interval [first_app_date, first_app_date + 14 days]).
-- ============================================================================

WITH site_first_applications AS (
    SELECT
        site_name,
        MIN(created_at) AS first_application_date
    FROM application
    GROUP BY site_name
)
SELECT
    a.site_name,
    s.first_application_date,
    COUNT(a.id) AS applications_in_first_14_days
FROM application a
INNER JOIN site_first_applications s
    ON a.site_name = s.site_name
WHERE a.created_at >= s.first_application_date
  AND a.created_at <= DATE_ADD(s.first_application_date, INTERVAL 14 DAY)
GROUP BY a.site_name, s.first_application_date
HAVING COUNT(a.id) >= 10
ORDER BY applications_in_first_14_days DESC, a.site_name ASC;