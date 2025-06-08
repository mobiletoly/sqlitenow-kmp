package dev.goquick.sqlitenow.samplekmp.generated

import dev.goquick.sqlitenow.samplekmp.model.AddressType
import kotlin.Boolean
import kotlin.Int
import kotlin.String
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime

/**
 * @param personId From Person.id
 * @param totalPersonCount From .total_person_count
 * @param first_name From Person.first_name
 * @param anotherLastName From Person.last_name
 * @param email From Person.email
 * @param phone From Person.phone
 * @param birthDate From Person.birth_date
 * @param personCreatedAt From Person.created_at
 * @param addressId From PersonAddress.id
 * @param addressType From PersonAddress.address_type
 * @param street From PersonAddress.street
 * @param city From PersonAddress.city
 * @param state From PersonAddress.state
 * @param postalCode From PersonAddress.postal_code
 * @param country From PersonAddress.country
 * @param isPrimary From PersonAddress.is_primary
 * @param addressCreatedAt From PersonAddress.created_at
 * @param fullName From .last_name
 */
public data class SelectPersonWithAddress(
    /**
     * From Person.id
     */
    public val personId: Int,
    /**
     * From .total_person_count
     */
    public val totalPersonCount: Int,
    /**
     * From Person.first_name
     */
    public val first_name: String?,
    /**
     * From Person.last_name
     */
    public val anotherLastName: String,
    /**
     * From Person.email
     */
    public val email: String,
    /**
     * From Person.phone
     */
    public val phone: String?,
    /**
     * From Person.birth_date
     */
    public val birthDate: String?,
    /**
     * From Person.created_at
     */
    public val personCreatedAt: LocalDateTime,
    /**
     * From PersonAddress.id
     */
    public val addressId: Int,
    /**
     * From PersonAddress.address_type
     */
    public val addressType: AddressType,
    /**
     * From PersonAddress.street
     */
    public val street: String,
    /**
     * From PersonAddress.city
     */
    public val city: String,
    /**
     * From PersonAddress.state
     */
    public val state: String?,
    /**
     * From PersonAddress.postal_code
     */
    public val postalCode: String?,
    /**
     * From PersonAddress.country
     */
    public val country: Int,
    /**
     * From PersonAddress.is_primary
     */
    public val isPrimary: Boolean,
    /**
     * From PersonAddress.created_at
     */
    public val addressCreatedAt: LocalDate,
    /**
     * From .last_name
     */
    public val fullName: String,
)
