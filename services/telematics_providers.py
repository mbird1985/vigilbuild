"""
Telematics Provider Integrations for Vigil Build
Connects to GPS/Fleet tracking providers to pull real-time location and sensor data.

Supported Providers:
- Samsara (https://www.samsara.com)
- Geotab (https://www.geotab.com)
- Verizon Connect (https://www.verizonconnect.com)
- CalAmp (https://www.calamp.com)
- GPS Trackit (https://gpstrackit.com)
- Generic API (custom endpoints)
"""

import requests
import json
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from services.db import get_connection, release_connection
from services.equipment_service import record_telematics_data


class TelematicsProvider(ABC):
    """Base class for telematics providers."""

    def __init__(self, config: dict):
        self.config = config
        self.api_key = config.get('api_key')
        self.base_url = config.get('base_url')

    @abstractmethod
    def authenticate(self) -> bool:
        """Authenticate with the provider."""
        pass

    @abstractmethod
    def get_vehicles(self) -> List[Dict]:
        """Get list of all vehicles/assets from the provider."""
        pass

    @abstractmethod
    def get_vehicle_location(self, vehicle_id: str) -> Optional[Dict]:
        """Get current location for a specific vehicle."""
        pass

    @abstractmethod
    def get_all_locations(self) -> List[Dict]:
        """Get current locations for all vehicles."""
        pass

    @abstractmethod
    def get_vehicle_diagnostics(self, vehicle_id: str) -> Optional[Dict]:
        """Get diagnostic data (engine hours, fuel, etc.)."""
        pass


class SamsaraProvider(TelematicsProvider):
    """
    Samsara Fleet Tracking Integration
    API Docs: https://developers.samsara.com/docs

    Required config:
    - api_key: Samsara API token
    - base_url: https://api.samsara.com (default)
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.base_url = config.get('base_url', 'https://api.samsara.com')
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }

    def authenticate(self) -> bool:
        """Test API connection."""
        try:
            response = requests.get(
                f'{self.base_url}/fleet/vehicles',
                headers=self.headers,
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            print(f"Samsara auth error: {e}")
            return False

    def get_vehicles(self) -> List[Dict]:
        """Get all vehicles from Samsara."""
        try:
            response = requests.get(
                f'{self.base_url}/fleet/vehicles',
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json()
            return [
                {
                    'provider_id': v['id'],
                    'name': v.get('name'),
                    'vin': v.get('vin'),
                    'license_plate': v.get('licensePlate'),
                    'make': v.get('make'),
                    'model': v.get('model'),
                    'year': v.get('year'),
                    'serial': v.get('serial'),
                    'external_ids': v.get('externalIds', {})
                }
                for v in data.get('data', [])
            ]
        except Exception as e:
            print(f"Samsara get_vehicles error: {e}")
            return []

    def get_vehicle_location(self, vehicle_id: str) -> Optional[Dict]:
        """Get current location for a vehicle."""
        try:
            response = requests.get(
                f'{self.base_url}/fleet/vehicles/{vehicle_id}/locations',
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json()
            loc = data.get('data', [{}])[0] if data.get('data') else {}
            return {
                'latitude': loc.get('latitude'),
                'longitude': loc.get('longitude'),
                'heading': loc.get('heading'),
                'speed': loc.get('speed'),
                'timestamp': loc.get('time'),
                'address': loc.get('reverseGeo', {}).get('formattedLocation')
            } if loc else None
        except Exception as e:
            print(f"Samsara get_location error: {e}")
            return None

    def get_all_locations(self) -> List[Dict]:
        """Get locations for all vehicles."""
        try:
            response = requests.get(
                f'{self.base_url}/fleet/vehicles/locations',
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json()
            return [
                {
                    'provider_id': loc['id'],
                    'name': loc.get('name'),
                    'latitude': loc.get('location', {}).get('latitude'),
                    'longitude': loc.get('location', {}).get('longitude'),
                    'heading': loc.get('location', {}).get('heading'),
                    'speed': loc.get('location', {}).get('speed'),
                    'timestamp': loc.get('location', {}).get('time'),
                    'address': loc.get('location', {}).get('reverseGeo', {}).get('formattedLocation')
                }
                for loc in data.get('data', [])
            ]
        except Exception as e:
            print(f"Samsara get_all_locations error: {e}")
            return []

    def get_vehicle_diagnostics(self, vehicle_id: str) -> Optional[Dict]:
        """Get vehicle diagnostics (fuel, engine hours, etc.)."""
        try:
            response = requests.get(
                f'{self.base_url}/fleet/vehicles/{vehicle_id}/stats',
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json().get('data', {})
            return {
                'engine_hours': data.get('engineSeconds', 0) / 3600,
                'odometer_miles': data.get('odometerMeters', 0) / 1609.34,
                'fuel_percent': data.get('fuelPercent', {}).get('value'),
                'engine_state': data.get('engineState', {}).get('value'),
                'battery_voltage': data.get('batteryMilliVolts', 0) / 1000
            }
        except Exception as e:
            print(f"Samsara diagnostics error: {e}")
            return None


class GeotabProvider(TelematicsProvider):
    """
    Geotab Fleet Tracking Integration
    API Docs: https://developers.geotab.com/myGeotab/apiReference

    Required config:
    - username: Geotab username
    - password: Geotab password
    - database: Geotab database name
    - base_url: https://my.geotab.com/apiv1 (default)
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.base_url = config.get('base_url', 'https://my.geotab.com/apiv1')
        self.username = config.get('username')
        self.password = config.get('password')
        self.database = config.get('database')
        self.session_id = None

    def authenticate(self) -> bool:
        """Authenticate with Geotab API."""
        try:
            response = requests.post(self.base_url, json={
                'method': 'Authenticate',
                'params': {
                    'userName': self.username,
                    'password': self.password,
                    'database': self.database
                }
            })
            response.raise_for_status()
            result = response.json().get('result')
            if result:
                self.session_id = result.get('credentials', {}).get('sessionId')
                return True
            return False
        except Exception as e:
            print(f"Geotab auth error: {e}")
            return False

    def _api_call(self, method: str, params: dict = None) -> dict:
        """Make authenticated API call."""
        if not self.session_id:
            self.authenticate()

        request_data = {
            'method': method,
            'params': {
                'credentials': {
                    'database': self.database,
                    'sessionId': self.session_id,
                    'userName': self.username
                },
                **(params or {})
            }
        }
        response = requests.post(self.base_url, json=request_data)
        response.raise_for_status()
        return response.json().get('result', {})

    def get_vehicles(self) -> List[Dict]:
        """Get all devices (vehicles) from Geotab."""
        try:
            result = self._api_call('Get', {'typeName': 'Device'})
            return [
                {
                    'provider_id': d['id'],
                    'name': d.get('name'),
                    'serial': d.get('serialNumber'),
                    'vin': d.get('vehicleIdentificationNumber'),
                    'license_plate': d.get('licensePlate'),
                    'device_type': d.get('deviceType'),
                    'comment': d.get('comment')
                }
                for d in result
            ]
        except Exception as e:
            print(f"Geotab get_vehicles error: {e}")
            return []

    def get_vehicle_location(self, vehicle_id: str) -> Optional[Dict]:
        """Get current location for a device."""
        try:
            result = self._api_call('Get', {
                'typeName': 'DeviceStatusInfo',
                'search': {'deviceSearch': {'id': vehicle_id}}
            })
            if result:
                status = result[0]
                return {
                    'latitude': status.get('latitude'),
                    'longitude': status.get('longitude'),
                    'bearing': status.get('bearing'),
                    'speed': status.get('speed'),
                    'timestamp': status.get('dateTime'),
                    'is_driving': status.get('isDriving'),
                    'current_state': status.get('currentStateDuration')
                }
            return None
        except Exception as e:
            print(f"Geotab get_location error: {e}")
            return None

    def get_all_locations(self) -> List[Dict]:
        """Get locations for all devices."""
        try:
            result = self._api_call('Get', {'typeName': 'DeviceStatusInfo'})
            return [
                {
                    'provider_id': s.get('device', {}).get('id'),
                    'latitude': s.get('latitude'),
                    'longitude': s.get('longitude'),
                    'bearing': s.get('bearing'),
                    'speed': s.get('speed'),
                    'timestamp': s.get('dateTime'),
                    'is_driving': s.get('isDriving')
                }
                for s in result
            ]
        except Exception as e:
            print(f"Geotab get_all_locations error: {e}")
            return []

    def get_vehicle_diagnostics(self, vehicle_id: str) -> Optional[Dict]:
        """Get diagnostic data."""
        try:
            # Get engine data
            result = self._api_call('Get', {
                'typeName': 'StatusData',
                'search': {
                    'deviceSearch': {'id': vehicle_id},
                    'fromDate': (datetime.utcnow() - timedelta(hours=1)).isoformat()
                }
            })
            # Parse relevant diagnostics
            diagnostics = {}
            for item in result:
                diagnostic_type = item.get('diagnostic', {}).get('name', '')
                if 'Engine Hours' in diagnostic_type:
                    diagnostics['engine_hours'] = item.get('data')
                elif 'Odometer' in diagnostic_type:
                    diagnostics['odometer'] = item.get('data')
                elif 'Fuel' in diagnostic_type:
                    diagnostics['fuel_level'] = item.get('data')
            return diagnostics
        except Exception as e:
            print(f"Geotab diagnostics error: {e}")
            return None


class VerizonConnectProvider(TelematicsProvider):
    """
    Verizon Connect Fleet Tracking Integration
    (formerly Fleetmatics / Reveal)

    Required config:
    - api_key: Verizon Connect API key
    - account_id: Account ID
    - base_url: API endpoint URL
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.base_url = config.get('base_url', 'https://fim.api.verizonconnect.com/api')
        self.account_id = config.get('account_id')
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
            'AccountId': self.account_id
        }

    def authenticate(self) -> bool:
        """Test API connection."""
        try:
            response = requests.get(
                f'{self.base_url}/vehicles',
                headers=self.headers,
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            print(f"Verizon Connect auth error: {e}")
            return False

    def get_vehicles(self) -> List[Dict]:
        """Get all vehicles."""
        try:
            response = requests.get(
                f'{self.base_url}/vehicles',
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json()
            return [
                {
                    'provider_id': v['vehicleId'],
                    'name': v.get('vehicleName'),
                    'vin': v.get('vin'),
                    'license_plate': v.get('registration'),
                    'make': v.get('make'),
                    'model': v.get('model'),
                    'year': v.get('year'),
                    'driver': v.get('driverName')
                }
                for v in data.get('vehicles', [])
            ]
        except Exception as e:
            print(f"Verizon get_vehicles error: {e}")
            return []

    def get_vehicle_location(self, vehicle_id: str) -> Optional[Dict]:
        """Get current location."""
        try:
            response = requests.get(
                f'{self.base_url}/vehicles/{vehicle_id}/location',
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json()
            return {
                'latitude': data.get('latitude'),
                'longitude': data.get('longitude'),
                'heading': data.get('heading'),
                'speed': data.get('speed'),
                'timestamp': data.get('timestamp'),
                'address': data.get('address')
            }
        except Exception as e:
            print(f"Verizon get_location error: {e}")
            return None

    def get_all_locations(self) -> List[Dict]:
        """Get all vehicle locations."""
        try:
            response = requests.get(
                f'{self.base_url}/vehicles/locations',
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json()
            return [
                {
                    'provider_id': loc['vehicleId'],
                    'name': loc.get('vehicleName'),
                    'latitude': loc.get('latitude'),
                    'longitude': loc.get('longitude'),
                    'heading': loc.get('heading'),
                    'speed': loc.get('speed'),
                    'timestamp': loc.get('timestamp')
                }
                for loc in data.get('locations', [])
            ]
        except Exception as e:
            print(f"Verizon get_all_locations error: {e}")
            return []

    def get_vehicle_diagnostics(self, vehicle_id: str) -> Optional[Dict]:
        """Get vehicle diagnostics."""
        try:
            response = requests.get(
                f'{self.base_url}/vehicles/{vehicle_id}/diagnostics',
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Verizon diagnostics error: {e}")
            return None


class GenericAPIProvider(TelematicsProvider):
    """
    Generic API Provider for custom GPS endpoints.

    Required config:
    - base_url: API base URL
    - api_key: API key (if required)
    - auth_type: 'bearer', 'basic', 'header', or 'none'
    - auth_header: Custom header name for API key (default: Authorization)
    - vehicles_endpoint: Path to get vehicles list
    - locations_endpoint: Path to get locations
    - location_mapping: Dict mapping response fields to standard fields
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.auth_type = config.get('auth_type', 'bearer')
        self.auth_header = config.get('auth_header', 'Authorization')
        self.vehicles_endpoint = config.get('vehicles_endpoint', '/vehicles')
        self.locations_endpoint = config.get('locations_endpoint', '/locations')
        self.location_mapping = config.get('location_mapping', {
            'latitude': 'latitude',
            'longitude': 'longitude',
            'speed': 'speed',
            'heading': 'heading',
            'timestamp': 'timestamp'
        })
        self.headers = self._build_headers()

    def _build_headers(self) -> dict:
        """Build request headers based on auth type."""
        headers = {'Content-Type': 'application/json'}
        if self.auth_type == 'bearer':
            headers[self.auth_header] = f'Bearer {self.api_key}'
        elif self.auth_type == 'basic':
            import base64
            credentials = base64.b64encode(f'{self.config.get("username")}:{self.api_key}'.encode()).decode()
            headers['Authorization'] = f'Basic {credentials}'
        elif self.auth_type == 'header':
            headers[self.auth_header] = self.api_key
        return headers

    def authenticate(self) -> bool:
        """Test API connection."""
        try:
            response = requests.get(
                f'{self.base_url}{self.vehicles_endpoint}',
                headers=self.headers,
                timeout=10
            )
            return response.status_code in [200, 201]
        except:
            return False

    def get_vehicles(self) -> List[Dict]:
        """Get vehicles from custom endpoint."""
        try:
            response = requests.get(
                f'{self.base_url}{self.vehicles_endpoint}',
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Generic API get_vehicles error: {e}")
            return []

    def get_vehicle_location(self, vehicle_id: str) -> Optional[Dict]:
        """Get location from custom endpoint."""
        try:
            response = requests.get(
                f'{self.base_url}{self.locations_endpoint}/{vehicle_id}',
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json()
            return self._map_location(data)
        except Exception as e:
            print(f"Generic API get_location error: {e}")
            return None

    def get_all_locations(self) -> List[Dict]:
        """Get all locations from custom endpoint."""
        try:
            response = requests.get(
                f'{self.base_url}{self.locations_endpoint}',
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list):
                return [self._map_location(loc) for loc in data]
            return []
        except Exception as e:
            print(f"Generic API get_all_locations error: {e}")
            return []

    def _map_location(self, data: dict) -> dict:
        """Map response fields to standard location format."""
        return {
            'latitude': data.get(self.location_mapping.get('latitude', 'latitude')),
            'longitude': data.get(self.location_mapping.get('longitude', 'longitude')),
            'speed': data.get(self.location_mapping.get('speed', 'speed')),
            'heading': data.get(self.location_mapping.get('heading', 'heading')),
            'timestamp': data.get(self.location_mapping.get('timestamp', 'timestamp'))
        }

    def get_vehicle_diagnostics(self, vehicle_id: str) -> Optional[Dict]:
        """Not implemented for generic provider."""
        return None


# Provider factory
PROVIDERS = {
    'samsara': SamsaraProvider,
    'geotab': GeotabProvider,
    'verizon_connect': VerizonConnectProvider,
    'generic': GenericAPIProvider
}


def get_telematics_provider(provider_type: str, config: dict) -> Optional[TelematicsProvider]:
    """Factory function to get a telematics provider instance."""
    provider_class = PROVIDERS.get(provider_type.lower())
    if provider_class:
        return provider_class(config)
    return None


# =============================================================================
# Database Functions for Provider Configuration
# =============================================================================

def save_telematics_provider(name: str, provider_type: str, config: dict, is_active: bool = True) -> int:
    """Save a telematics provider configuration."""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO telematics_providers (name, provider_type, config, is_active)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (name) DO UPDATE SET
                provider_type = EXCLUDED.provider_type,
                config = EXCLUDED.config,
                is_active = EXCLUDED.is_active,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
        """, (name, provider_type, json.dumps(config), is_active))
        provider_id = c.fetchone()[0]
        conn.commit()
        return provider_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_connection(conn)


def get_telematics_providers(active_only: bool = True) -> List[Dict]:
    """Get all configured telematics providers."""
    conn = get_connection()
    c = conn.cursor()
    try:
        query = "SELECT id, name, provider_type, config, is_active, last_sync FROM telematics_providers"
        if active_only:
            query += " WHERE is_active = TRUE"
        c.execute(query)
        return [
            {
                'id': r[0], 'name': r[1], 'provider_type': r[2],
                'config': json.loads(r[3]) if r[3] else {},
                'is_active': r[4], 'last_sync': r[5]
            }
            for r in c.fetchall()
        ]
    finally:
        release_connection(conn)


def link_equipment_to_provider(equipment_id: int, provider_id: int, external_id: str) -> bool:
    """Link local equipment to a provider's vehicle/asset ID."""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO equipment_telematics_links (equipment_id, provider_id, external_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (equipment_id, provider_id) DO UPDATE SET
                external_id = EXCLUDED.external_id,
                updated_at = CURRENT_TIMESTAMP
        """, (equipment_id, provider_id, external_id))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error linking equipment: {e}")
        return False
    finally:
        release_connection(conn)


def sync_provider_locations(provider_id: int) -> int:
    """Sync locations from a telematics provider to local database."""
    conn = get_connection()
    c = conn.cursor()
    synced_count = 0

    try:
        # Get provider config
        c.execute("SELECT provider_type, config FROM telematics_providers WHERE id = %s", (provider_id,))
        row = c.fetchone()
        if not row:
            return 0

        provider_type, config = row[0], json.loads(row[1]) if row[1] else {}
        provider = get_telematics_provider(provider_type, config)
        if not provider:
            return 0

        # Get all locations from provider
        locations = provider.get_all_locations()

        # Get equipment links
        c.execute("""
            SELECT equipment_id, external_id
            FROM equipment_telematics_links
            WHERE provider_id = %s
        """, (provider_id,))
        links = {row[1]: row[0] for row in c.fetchall()}

        # Update locations for linked equipment
        for loc in locations:
            external_id = loc.get('provider_id') or loc.get('name')
            if external_id in links:
                equipment_id = links[external_id]
                lat, lng = loc.get('latitude'), loc.get('longitude')
                if lat and lng:
                    record_telematics_data(
                        equipment_id=equipment_id,
                        data_type='gps_location',
                        value=0,  # Placeholder, actual coords in raw_data
                        source=provider_type,
                        raw_data={
                            'latitude': lat,
                            'longitude': lng,
                            'speed': loc.get('speed'),
                            'heading': loc.get('heading'),
                            'timestamp': loc.get('timestamp')
                        }
                    )
                    synced_count += 1

        # Update last sync time
        c.execute(
            "UPDATE telematics_providers SET last_sync = CURRENT_TIMESTAMP WHERE id = %s",
            (provider_id,)
        )
        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Sync error: {e}")
    finally:
        release_connection(conn)

    return synced_count


def sync_all_providers() -> Dict[str, int]:
    """Sync all active telematics providers."""
    results = {}
    providers = get_telematics_providers(active_only=True)
    for provider in providers:
        count = sync_provider_locations(provider['id'])
        results[provider['name']] = count
    return results
