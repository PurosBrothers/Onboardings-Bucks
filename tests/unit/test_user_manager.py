import os
from dotenv import load_dotenv
from argon2 import PasswordHasher, exceptions as argon2_exceptions
import bcrypt
import logging
from src.user_manager import UserManager
from src.models.user import User
import pytest
import pytest_asyncio
from src.config.beanie_config import init_db

logger = logging.getLogger("test_logger")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Test constants - these will be used if env vars are not set
TEST_CONSTANTS = {
    "TEST_PASSWORD": "TestPassword123!",
    "TEST_EMAIL": "test@example.com",
    "TEST_NAME": "Test",
    "TEST_LASTNAME": "User",
    "TEST_PHONE": "1234567890",
    "TEST_PASSWORD_HASH": "$argon2id$v=19$m=65536,t=3,p=4$c2FsdHlzYWx0$dGVzdGhhc2g",  # This is a dummy hash
    "TEST_NUM_CONSECUTIVO": "1"  # Default value
}

def get_test_value(key: str) -> str:
    """Get test value with logging"""
    env_value = os.getenv(key)
    if env_value:
        logger.info(f"Using environment value for {key}: {env_value}")
        return env_value
    test_value = TEST_CONSTANTS[key]
    logger.info(f"Using test constant for {key}: {test_value}")
    return test_value

@pytest.fixture(autouse=True)
def setup_test_env(monkeypatch):
    """Setup test environment variables"""
    logger.info("Setting up test environment...")
    
    # Try to load from .env first
    load_dotenv()
    logger.info("Loaded .env file")
    
    # Set test constants if env vars are not set
    for key, value in TEST_CONSTANTS.items():
        current_value = os.getenv(key)
        if not current_value:
            logger.info(f"Setting {key} to test value: {value}")
            monkeypatch.setenv(key, value)
        else:
            logger.info(f"Using existing value for {key}: {current_value}")

    # Verify all variables are set
    for key in TEST_CONSTANTS:
        value = os.getenv(key)
        logger.info(f"Final value for {key}: {value}")
        assert value is not None, f"Environment variable {key} is not set"
        assert value != "", f"Environment variable {key} is empty"

# Get environment variables with fallback to test constants
TEST_PASSWORD = get_test_value("TEST_PASSWORD")
TEST_EMAIL = get_test_value("TEST_EMAIL")
TEST_NAME = get_test_value("TEST_NAME")
TEST_LASTNAME = get_test_value("TEST_LASTNAME")
TEST_PHONE = get_test_value("TEST_PHONE")
TEST_PASSWORD_HASH = get_test_value("TEST_PASSWORD_HASH")
TEST_NUM_CONSECUTIVO = int(get_test_value("TEST_NUM_CONSECUTIVO"))

def test_env_variables():
    """Verify that environment variables are loaded correctly"""
    logger.info("Testing environment variables...")
    logger.info(f"TEST_PASSWORD: {TEST_PASSWORD}")
    logger.info(f"TEST_EMAIL: {TEST_EMAIL}")
    logger.info(f"TEST_NAME: {TEST_NAME}")
    logger.info(f"TEST_LASTNAME: {TEST_LASTNAME}")
    logger.info(f"TEST_PHONE: {TEST_PHONE}")
    logger.info(f"TEST_PASSWORD_HASH: {TEST_PASSWORD_HASH}")
    
    assert TEST_PASSWORD is not None and TEST_PASSWORD != "", "TEST_PASSWORD is not set"
    assert TEST_EMAIL is not None and TEST_EMAIL != "", "TEST_EMAIL is not set"
    assert TEST_NAME is not None and TEST_NAME != "", "TEST_NAME is not set"
    assert TEST_LASTNAME is not None and TEST_LASTNAME != "", "TEST_LASTNAME is not set"
    assert TEST_PHONE is not None and TEST_PHONE != "", "TEST_PHONE is not set"
    assert TEST_PASSWORD_HASH is not None and TEST_PASSWORD_HASH != "", "TEST_PASSWORD_HASH is not set"
    
    # Additional validation
    assert "@" in TEST_EMAIL, f"TEST_EMAIL must contain @ symbol: {TEST_EMAIL}"
    assert len(TEST_PASSWORD) >= 8, f"TEST_PASSWORD must be at least 8 characters: {TEST_PASSWORD}"

ph = PasswordHasher()

@pytest_asyncio.fixture(autouse=True)
async def setup_database():
    """Initialize database connection"""
    await init_db(document_models=[User])
    yield

@pytest_asyncio.fixture(autouse=True)
async def cleanup_test_user():
    """
    Fixture that runs automatically before and after each test to clean up test users.
    The autouse=True parameter means this fixture will run automatically for all tests.
    """

    await init_db(document_models=[User])

    print("🔵 BEFORE: Cleaning up any existing test user")  # Runs first
    existing_user = await User.find_one({"email": TEST_EMAIL})
    if existing_user:
        await existing_user.delete()
    
    yield  # Test runs here
    
    print("🔴 AFTER: Cleaning up test user")  # Runs last
    existing_user = await User.find_one({"email": TEST_EMAIL})
    if existing_user:
        await existing_user.delete()

@pytest_asyncio.fixture
async def user_creator():
    """
    Fixture para crear una instancia de UserService con datos de prueba cargados desde .env.
    
    Retorna:
        UserManager: Instancia preparada para usar en tests.
    """
    logger.info(f"Creating UserManager with: name={TEST_NAME}, email={TEST_EMAIL}, num_consecutivo={TEST_NUM_CONSECUTIVO}")
    
    assert TEST_NAME is not None, "TEST_NAME is None"
    assert TEST_LASTNAME is not None, "TEST_LASTNAME is None"
    assert TEST_EMAIL is not None, "TEST_EMAIL is None"
    assert TEST_PHONE is not None, "TEST_PHONE is None"
    assert TEST_PASSWORD is not None, "TEST_PASSWORD is None"
    assert TEST_NUM_CONSECUTIVO is not None, "TEST_NUM_CONSECUTIVO is None"

    return UserManager(
        name=TEST_NAME,
        lastname=TEST_LASTNAME,
        email=TEST_EMAIL,
        phone=TEST_PHONE,
        password_plain=TEST_PASSWORD,
        num_consecutivo=TEST_NUM_CONSECUTIVO
    )

@pytest.mark.asyncio
async def test_create_user_structure():
    """
    Verifica que el método create_user() devuelva un diccionario con la estructura y valores esperados.
    También confirma que los datos del usuario coinciden con los cargados desde .env.
    """
    await init_db(document_models=[User])

    logger.info("Ejecutando test_create_user_structure")
    creator = UserManager(
        name=TEST_NAME,
        lastname=TEST_LASTNAME,
        email=TEST_EMAIL,
        phone=TEST_PHONE,
        password_plain=TEST_PASSWORD,
        num_consecutivo=TEST_NUM_CONSECUTIVO
    )
    user = await creator.create_user()
    print(type(user), user)
    assert isinstance(user, User)
    assert user.email == TEST_EMAIL
    assert user.password is not None
    assert user.newUser is True
    assert user.verified is True
    assert user.name == TEST_NAME
    assert user.phone == TEST_PHONE
    assert user.num_consecutivo == TEST_NUM_CONSECUTIVO
    logger.info("test_create_user_structure completado correctamente")

@pytest.mark.asyncio
async def test_password_is_hashed_correctly(user_creator):
    """
    Comprueba que la contraseña almacenada en el usuario creado es válida para Argon2
    y corresponde con la contraseña de prueba (TEST_PASSWORD).
    """
    logger.info("Ejecutando test_password_is_hashed_correctly")
    user = await user_creator.create_user()
    hashed_password = user.password
    assert ph.verify(hashed_password, TEST_PASSWORD)
    logger.info("test_password_is_hashed_correctly completado correctamente")

@pytest.mark.asyncio
@pytest.mark.parametrize("input_password,expected", [
    (TEST_PASSWORD, True),
    ("IncorrectPass123", False)
])

async def test_password_verification(user_creator, input_password, expected):
    """
    Test parametrizado para verificar que la función de verificación de contraseña
    funciona correctamente para contraseñas correctas e incorrectas.

    Args:
        input_password (str): Contraseña a probar.
        expected (bool): Resultado esperado de la verificación (True si debe pasar, False si debe fallar).
    """
    logger.info(f"Probando verificación con password: {input_password}")
    user = await user_creator.create_user()
    hashed_password = user.password
    try:
        result = ph.verify(hashed_password, input_password)
        assert result == expected
    except argon2_exceptions.VerifyMismatchError:
        assert not expected
    logger.info(f"Verificación con password '{input_password}' resultó: {expected}")


def test_bcrypt_hash_fails_argon2_verification():
    """
    Verifica que un hash generado con bcrypt no es válido para la verificación con Argon2.
    Esto asegura que la función de verificación distingue correctamente entre hashes Argon2 y bcrypt.
    """
    bcrypt_hash = bcrypt.hashpw(TEST_PASSWORD.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    try:
        ph.verify(bcrypt_hash, TEST_PASSWORD)
        assert False, "No debería validar un hash bcrypt como si fuera Argon2"
    except argon2_exceptions.InvalidHash:
        assert True

def test_env_hash_matches_plain_password():
    """
    Verifica que el hash cargado desde .env fue generado con la contraseña en texto plano.
    Esto valida que el hash corresponde correctamente a la contraseña esperada.
    """
    logger.info("Ejecutando test_env_hash_matches_plain_password")
    assert TEST_PASSWORD_HASH is not None, "TEST_PASSWORD_HASH no está definido en el .env"
    assert TEST_PASSWORD is not None, "TEST_PASSWORD no está definido en el .env"
    assert ph.verify(TEST_PASSWORD_HASH, TEST_PASSWORD)
    logger.info("test_env_hash_matches_plain_password completado correctamente")
